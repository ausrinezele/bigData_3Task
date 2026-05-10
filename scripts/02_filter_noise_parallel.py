#!/usr/bin/env python3
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from pymongo import MongoClient
from pymongo.errors import BulkWriteError, OperationFailure
from tqdm import tqdm


MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ais"
SOURCE_COLLECTION = "raw_positions"
TARGET_COLLECTION = "filtered_positions"


def valid_record_match():
    return {
        "Navigational status": {"$nin": [None, "", "Unknown"]},
        "MMSI": {"$type": "number", "$gte": 100000000, "$lte": 999999999},
        "Latitude": {"$type": "number", "$gte": -90, "$lte": 90},
        "Longitude": {"$type": "number", "$gte": -180, "$lte": 180},
        "ROT": {"$type": "number"},
        "SOG": {"$type": "number", "$gte": 0, "$lte": 102.2},
        "COG": {"$type": "number", "$gte": 0, "$lt": 360},
        "Heading": {"$type": "number", "$gte": 0, "$lte": 359},
        "timestamp": {"$type": "date"},
    }


def ensure_sharded_collection(mongo_uri, db_name, collection_name):
    client = MongoClient(mongo_uri)
    collection = client[db_name][collection_name]
    namespace = f"{db_name}.{collection_name}"

    try:
        client.admin.command("enableSharding", db_name)
    except OperationFailure as exc:
        if exc.code not in (23,):
            raise

    collection.create_index([("MMSI", "hashed")])

    try:
        client.admin.command(
            {
                "shardCollection": namespace,
                "key": {"MMSI": "hashed"},
            }
        )
        print(f"{namespace} sharded by {{ MMSI: 'hashed' }}")
    except OperationFailure as exc:
        message = str(exc)
        if "already sharded" in message or "Already sharded" in message:
            print(f"{namespace} already sharded")
        else:
            raise
    finally:
        client.close()


def create_indexes(mongo_uri, db_name, source_collection, target_collection):
    client = MongoClient(mongo_uri)
    db = client[db_name]

    db[source_collection].create_index([("MMSI", 1), ("timestamp", 1)])
    db[source_collection].create_index([("MMSI", 1)])
    db[target_collection].create_index([("MMSI", "hashed")])
    db[target_collection].create_index([("MMSI", 1), ("timestamp", 1)])
    db[target_collection].create_index([("timestamp", 1)])

    client.close()


def drop_collection(mongo_uri, db_name, collection_name):
    client = MongoClient(mongo_uri)
    client[db_name][collection_name].drop()
    client.close()


def find_valid_mmsis(mongo_uri, db_name, source_collection, min_points):
    client = MongoClient(mongo_uri)
    collection = client[db_name][source_collection]

    pipeline = [
        {"$match": valid_record_match()},
        {"$group": {"_id": "$MMSI", "points": {"$sum": 1}}},
        {"$match": {"points": {"$gte": min_points}}},
        {"$project": {"_id": 0, "MMSI": "$_id", "points": 1}},
        {"$sort": {"MMSI": 1}},
    ]
    #ets MongoDB use temporary disk space for large aggregation
    vessels = list(collection.aggregate(pipeline, allowDiskUse=True)) 
    client.close()
    return vessels


def chunks(items, chunk_size):
    for index in range(0, len(items), chunk_size):
        yield items[index : index + chunk_size]


def filter_vessel_chunk(
    mmsis,
    mongo_uri,
    db_name,
    source_collection,
    target_collection,
    insert_batch_size,
):
    # each parallel worker creates its own MongoDB client
    client = MongoClient(mongo_uri)
    db = client[db_name]
    source = db[source_collection]
    target = db[target_collection]
    copied = 0
    batch = []
    filtered_at = datetime.now(timezone.utc)

    query = {
        "$and": [
            valid_record_match(),
            {"MMSI": {"$in": mmsis}},
        ]
    }

    try:
        for document in source.find(query, no_cursor_timeout=True).sort(
            [("MMSI", 1), ("timestamp", 1)]
        ):
            document.pop("_id", None)
            document["filtered_at"] = filtered_at
            batch.append(document)

            if len(batch) >= insert_batch_size:
                copied += insert_documents(target, batch)
                batch = []

        if batch:
            copied += insert_documents(target, batch)

        return copied
    finally:
        client.close()


def insert_documents(collection, documents):
    try:
        result = collection.insert_many(documents, ordered=False)
        return len(result.inserted_ids)
    except BulkWriteError as exc:
        return exc.details.get("nInserted", 0)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Filter noisy AIS records into a separate MongoDB collection."
    )
    parser.add_argument("--mongo-uri", default=MONGO_URI, help="MongoDB URI.")
    parser.add_argument("--db", default=DB_NAME, help="MongoDB database name.")
    parser.add_argument(
        "--source", default=SOURCE_COLLECTION, help="Source collection name."
    )
    parser.add_argument(
        "--target", default=TARGET_COLLECTION, help="Target collection name."
    )
    parser.add_argument(
        "--min-points",
        type=int,
        default=100,
        help="Minimum valid points required for a vessel.",
    )
    parser.add_argument(
        "--workers", type=int, default=4, help="Number of parallel filter workers."
    )
    parser.add_argument(
        "--vessels-per-task",
        type=int,
        default=100,
        help="Number of MMSI values handled by each worker task.",
    )
    parser.add_argument(
        "--insert-batch-size",
        type=int,
        default=5_000,
        help="Documents per insert_many call.",
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop the target collection before filtering.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.drop:
        print(f"Dropping {args.db}.{args.target}...")
        drop_collection(args.mongo_uri, args.db, args.target)

    print(f"Ensuring {args.db}.{args.target} is sharded by hashed MMSI...")
    ensure_sharded_collection(args.mongo_uri, args.db, args.target)

    print("Creating indexes...")
    create_indexes(args.mongo_uri, args.db, args.source, args.target)

    print(f"Finding vessels with at least {args.min_points} valid data points...")
    vessels = find_valid_mmsis(
        args.mongo_uri,
        args.db,
        args.source,
        args.min_points,
    )
    valid_mmsis = [vessel["MMSI"] for vessel in vessels]
    valid_points = sum(vessel["points"] for vessel in vessels)

    print(f"Valid vessels: {len(valid_mmsis)}")
    print(f"Valid records to copy: {valid_points}")

    if not valid_mmsis:
        print("No vessels matched the filtering criteria.")
        return

    copied_total = 0
    mmsi_chunks = list(chunks(valid_mmsis, args.vessels_per_task))

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                filter_vessel_chunk,
                mmsi_chunk,
                args.mongo_uri,
                args.db,
                args.source,
                args.target,
                args.insert_batch_size,
            )
            for mmsi_chunk in mmsi_chunks
        ]

        with tqdm(total=valid_points, unit="docs") as progress:
            for completed in as_completed(futures):
                copied = completed.result()
                copied_total += copied
                progress.update(copied)

    print(f"Filtered documents inserted: {copied_total}")


if __name__ == "__main__":
    main()
