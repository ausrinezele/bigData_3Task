#!/usr/bin/env python3
import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from tqdm import tqdm


CSV_PATH = Path("data/aisdk-2026-04-18.csv")
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ais"
COLLECTION_NAME = "raw_positions"

TIMESTAMP_FIELD = "# Timestamp"
DATE_FORMAT = "%d/%m/%Y %H:%M:%S"

INTEGER_FIELDS = {
    "MMSI",
    "Heading",
    "IMO",
    "Width",
    "Length",
    "A",
    "B",
    "C",
    "D",
}

FLOAT_FIELDS = {
    "Latitude",
    "Longitude",
    "ROT",
    "SOG",
    "COG",
    "Draught",
}

# converts one CSV value into the correct type
def parse_value(field_name, value):
    value = value.strip()
    if value == "":
        return None

    if field_name == TIMESTAMP_FIELD:
        return value

    if field_name in INTEGER_FIELDS:
        try:
            return int(value)
        except ValueError:
            return None

    if field_name in FLOAT_FIELDS:
        try:
            return float(value)
        except ValueError:
            return None

    return value

# converts one CSV row into one MongoDB document
def row_to_document(row):
    document = {}

    for field_name, value in row.items():
        document[field_name] = parse_value(field_name, value or "")

    timestamp_text = document.get(TIMESTAMP_FIELD)
    if timestamp_text:
        try:
            document["timestamp"] = datetime.strptime(timestamp_text, DATE_FORMAT)
        except ValueError:
            document["timestamp"] = None
    else:
        document["timestamp"] = None

    return document


# inserts one batch of documents into MongoDB. each parallel task uses its own MongoClient.
def insert_batch(batch, mongo_uri, db_name, collection_name):
    client = MongoClient(mongo_uri)
    collection = client[db_name][collection_name]

    try:
        result = collection.insert_many(batch, ordered=False)
        return len(result.inserted_ids)
    except BulkWriteError as exc:
        return exc.details.get("nInserted", 0)
    finally:
        client.close() #worker closes its MongoDB connection

# reads the CSV file gradually
def batched_csv_documents(csv_path, batch_size, max_rows):
    batch = []
    rows_read = 0

    with csv_path.open("r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)

        for row in reader:
            batch.append(row_to_document(row))
            rows_read += 1

            if len(batch) >= batch_size:
                yield batch
                batch = []

            if max_rows is not None and rows_read >= max_rows:
                break

    if batch:
        yield batch

# creates indexes
def create_indexes(mongo_uri, db_name, collection_name):
    client = MongoClient(mongo_uri)
    collection = client[db_name][collection_name]

    collection.create_index([("MMSI", 1)])
    # timestamp turetu buti naudingas skaiciuojant delta t
    collection.create_index([("MMSI", 1), ("timestamp", 1)]) 
    collection.create_index([("timestamp", 1)])

    client.close()

# deletes the old collection
def drop_collection(mongo_uri, db_name, collection_name):
    client = MongoClient(mongo_uri)
    client[db_name][collection_name].drop()
    client.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import AIS CSV rows into MongoDB in parallel."
    )
    parser.add_argument("--csv", default=str(CSV_PATH), help="Path to AIS CSV file.")
    parser.add_argument("--mongo-uri", default=MONGO_URI, help="MongoDB URI.")
    parser.add_argument("--db", default=DB_NAME, help="MongoDB database name.")
    parser.add_argument(
        "--collection", default=COLLECTION_NAME, help="MongoDB collection name."
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=100_000,
        help="Maximum number of CSV rows to import. Use 0 for all rows.",
    )
    parser.add_argument(
        "--batch-size", type=int, default=5_000, help="Documents per insert task."
    )
    parser.add_argument(
        "--workers", type=int, default=4, help="Number of parallel insert workers."
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop the target collection before importing.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    csv_path = Path(args.csv)
    max_rows = None if args.max_rows == 0 else args.max_rows

    if not csv_path.exists():
        raise FileNotFoundError(
            f"{csv_path} does not exist. Run: bash scripts/download-data.sh"
        )

    if args.drop:
        print(f"Dropping {args.db}.{args.collection}...")
        drop_collection(args.mongo_uri, args.db, args.collection)

    print(f"Creating indexes for {args.db}.{args.collection}...")
    create_indexes(args.mongo_uri, args.db, args.collection)

    print(
        f"Importing from {csv_path} into {args.db}.{args.collection} "
        f"with {args.workers} workers..."
    )

    inserted_total = 0
    futures = set()
    progress_total = max_rows if max_rows is not None else None

    # in Parallel dalis
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        with tqdm(total=progress_total, unit="docs") as progress:
            for batch in batched_csv_documents(csv_path, args.batch_size, max_rows):
                future = executor.submit(
                    insert_batch,
                    batch,
                    args.mongo_uri,
                    args.db,
                    args.collection,
                )
                futures.add(future)

                if len(futures) >= args.workers * 2:
                    for completed in as_completed(futures):
                        inserted = completed.result()
                        inserted_total += inserted
                        progress.update(inserted)
                        futures.remove(completed)
                        break

            for completed in as_completed(futures):
                inserted = completed.result()
                inserted_total += inserted
                progress.update(inserted)

    print(f"Inserted documents: {inserted_total}")


if __name__ == "__main__":
    main()
