#!/usr/bin/env python3
import argparse
import csv
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median

from pymongo import MongoClient
from pymongo.errors import BulkWriteError, OperationFailure
from tqdm import tqdm

# Optional dependency only needed for PNG histogram output.
# Install with: pip install matplotlib
import matplotlib.pyplot as plt


MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ais"
SOURCE_COLLECTION = "filtered_positions"
DELTA_COLLECTION = "delta_t_positions"
DEFAULT_OUTPUT_DIR = "outputs"


def chunks(items, chunk_size):
    for index in range(0, len(items), chunk_size):
        yield items[index : index + chunk_size]


def drop_collection(mongo_uri, db_name, collection_name):
    client = MongoClient(mongo_uri)
    client[db_name][collection_name].drop()
    client.close()


def create_indexes(mongo_uri, db_name, source_collection, delta_collection):
    client = MongoClient(mongo_uri)
    db = client[db_name]

    # This index is essential because delta t must be calculated in MMSI + timestamp order.
    db[source_collection].create_index([("MMSI", 1), ("timestamp", 1)])
    db[delta_collection].create_index([("MMSI", "hashed")])
    db[delta_collection].create_index([("MMSI", 1), ("timestamp", 1)])
    db[delta_collection].create_index([("delta_t_ms", 1)])

    client.close()


def ensure_sharded_collection(mongo_uri, db_name, collection_name):
    client = MongoClient(mongo_uri)
    collection = client[db_name][collection_name]
    namespace = f"{db_name}.{collection_name}"

    try:
        client.admin.command("enableSharding", db_name)
    except OperationFailure as exc:
        # code 23 = already initialized/enabled in many MongoDB versions
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


def get_vessel_mmsis(mongo_uri, db_name, source_collection):
    client = MongoClient(mongo_uri)
    collection = client[db_name][source_collection]

    mmsis = collection.distinct("MMSI")
    mmsis = sorted(m for m in mmsis if isinstance(m, int))

    client.close()
    return mmsis


def count_source_documents(mongo_uri, db_name, source_collection):
    client = MongoClient(mongo_uri)
    count = client[db_name][source_collection].count_documents({})
    client.close()
    return count


def insert_documents(collection, documents):
    if not documents:
        return 0

    try:
        result = collection.insert_many(documents, ordered=False)
        return len(result.inserted_ids)
    except BulkWriteError as exc:
        return exc.details.get("nInserted", 0)


def calculate_delta_for_mmsi_chunk(
    mmsis,
    mongo_uri,
    db_name,
    source_collection,
    delta_collection,
    insert_batch_size,
):
    """
    For every MMSI in this chunk:
    - read its filtered AIS points sorted by timestamp
    - calculate delta_t_ms between current and previous point
    - insert one document per delta value into delta_collection
    """
    client = MongoClient(mongo_uri)
    db = client[db_name]
    source = db[source_collection]
    target = db[delta_collection]

    inserted = 0
    delta_values = []
    batch = []
    calculated_at = datetime.now(timezone.utc)

    try:
        for mmsi in mmsis:
            previous_timestamp = None

            cursor = (
                source.find(
                    {"MMSI": mmsi, "timestamp": {"$type": "date"}},
                    {"_id": 0, "MMSI": 1, "timestamp": 1, "Latitude": 1, "Longitude": 1},
                    no_cursor_timeout=True,
                )
                .sort("timestamp", 1)
            )

            try:
                for document in cursor:
                    current_timestamp = document.get("timestamp")

                    if previous_timestamp is not None:
                        delta_t_ms = int(
                            (current_timestamp - previous_timestamp).total_seconds()
                            * 1000
                        )

                        # Ignore duplicate/out-of-order timestamps. With correct sorting this mostly
                        # removes duplicate AIS messages at the same timestamp.
                        if delta_t_ms > 0:
                            delta_doc = {
                                "MMSI": mmsi,
                                "timestamp": current_timestamp,
                                "previous_timestamp": previous_timestamp,
                                "delta_t_ms": delta_t_ms,
                                "calculated_at": calculated_at,
                            }
                            batch.append(delta_doc)
                            delta_values.append(delta_t_ms)

                    previous_timestamp = current_timestamp

                    if len(batch) >= insert_batch_size:
                        inserted += insert_documents(target, batch)
                        batch = []
            finally:
                cursor.close()

        if batch:
            inserted += insert_documents(target, batch)

        return {
            "inserted": inserted,
            "delta_values": delta_values,
            "vessels": len(mmsis),
        }
    finally:
        client.close()


def write_delta_csv(delta_values, output_csv):
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["delta_t_ms"])
        for value in delta_values:
            writer.writerow([value])


def generate_histogram(delta_values, output_png, bins):
    output_png.parent.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(10, 6))
    plt.hist(delta_values, bins=bins)
    plt.xlabel("Delta t between subsequent AIS points (milliseconds)")
    plt.ylabel("Frequency")
    plt.title("Histogram of AIS delta t values")
    plt.tight_layout()
    plt.savefig(output_png, dpi=150)
    plt.close()


def write_summary(delta_values, output_txt):
    output_txt.parent.mkdir(parents=True, exist_ok=True)

    if not delta_values:
        summary = "No delta_t_ms values were calculated.\n"
    else:
        sorted_values = sorted(delta_values)
        n = len(sorted_values)

        def percentile(p):
            if n == 1:
                return sorted_values[0]
            rank = (p / 100) * (n - 1)
            lower = math.floor(rank)
            upper = math.ceil(rank)
            if lower == upper:
                return sorted_values[int(rank)]
            weight = rank - lower
            return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight

        summary = "\n".join(
            [
                "AIS delta t summary",
                "===================",
                f"Delta t count: {n}",
                f"Minimum: {min(sorted_values):.0f} ms",
                f"Maximum: {max(sorted_values):.0f} ms",
                f"Mean: {mean(sorted_values):.2f} ms",
                f"Median: {median(sorted_values):.2f} ms",
                f"25th percentile: {percentile(25):.2f} ms",
                f"75th percentile: {percentile(75):.2f} ms",
                f"95th percentile: {percentile(95):.2f} ms",
                "",
                "Interpretation template:",
                "- Small delta t values mean the vessel sends AIS messages frequently.",
                "- Large delta t values can mean lower reporting frequency, gaps in reception, or the vessel leaving/entering coverage.",
                "- A strong peak at a regular interval suggests a common AIS reporting interval in the filtered dataset.",
            ]
        )
        summary += "\n"

    output_txt.write_text(summary, encoding="utf-8")
    print(summary)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Calculate delta t between subsequent AIS points and generate a histogram."
    )
    parser.add_argument("--mongo-uri", default=MONGO_URI, help="MongoDB URI.")
    parser.add_argument("--db", default=DB_NAME, help="MongoDB database name.")
    parser.add_argument(
        "--source",
        default=SOURCE_COLLECTION,
        help="Source collection with filtered AIS points.",
    )
    parser.add_argument(
        "--delta-collection",
        default=DELTA_COLLECTION,
        help="Collection where delta t documents are stored.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel delta calculation workers.",
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
        "--bins",
        type=int,
        default=100,
        help="Number of bins in the histogram.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for histogram PNG, CSV, and summary TXT.",
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop the delta t collection before calculating.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)

    source_count = count_source_documents(args.mongo_uri, args.db, args.source)
    if source_count == 0:
        raise RuntimeError(
            f"No documents found in {args.db}.{args.source}. "
            "Run Task 2 import and Task 3 filtering first."
        )

    if args.drop:
        print(f"Dropping {args.db}.{args.delta_collection}...")
        drop_collection(args.mongo_uri, args.db, args.delta_collection)

    print(f"Ensuring {args.db}.{args.delta_collection} is sharded by hashed MMSI...")
    ensure_sharded_collection(args.mongo_uri, args.db, args.delta_collection)

    print("Creating indexes...")
    create_indexes(args.mongo_uri, args.db, args.source, args.delta_collection)

    print(f"Finding vessels in {args.db}.{args.source}...")
    mmsis = get_vessel_mmsis(args.mongo_uri, args.db, args.source)
    print(f"Vessels found: {len(mmsis)}")

    if not mmsis:
        raise RuntimeError("No MMSI values found in the filtered collection.")

    mmsi_chunks = list(chunks(mmsis, args.vessels_per_task))

    all_delta_values = []
    inserted_total = 0
    vessels_processed = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                calculate_delta_for_mmsi_chunk,
                mmsi_chunk,
                args.mongo_uri,
                args.db,
                args.source,
                args.delta_collection,
                args.insert_batch_size,
            )
            for mmsi_chunk in mmsi_chunks
        ]

        with tqdm(total=len(mmsis), unit="vessels") as progress:
            for completed in as_completed(futures):
                result = completed.result()
                inserted_total += result["inserted"]
                vessels_processed += result["vessels"]
                all_delta_values.extend(result["delta_values"])
                progress.update(result["vessels"])

    print(f"Vessels processed: {vessels_processed}")
    print(f"Delta t documents inserted into {args.db}.{args.delta_collection}: {inserted_total}")

    output_csv = output_dir / "delta_t_values.csv"
    output_png = output_dir / "delta_t_histogram.png"
    output_txt = output_dir / "delta_t_summary.txt"

    print(f"Writing delta values to {output_csv}...")
    write_delta_csv(all_delta_values, output_csv)

    print(f"Generating histogram at {output_png}...")
    generate_histogram(all_delta_values, output_png, args.bins)

    print(f"Writing summary to {output_txt}...")
    write_summary(all_delta_values, output_txt)


if __name__ == "__main__":
    main()
