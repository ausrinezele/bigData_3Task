# AIS MongoDB Noise Filtering Assignment

This project uses a MongoDB sharded cluster in Docker Compose for storing and processing AIS vessel data.

## Task 1: MongoDB Cluster

Cluster structure:

- `mongos`: query router exposed on `localhost:27017`
- `configsvr1`: config server replica set named `configReplSet`
- `shard1a`, `shard1b`: shard replica set named `shard1ReplSet`
- `shard2a`, `shard2b`: shard replica set named `shard2ReplSet`

Applications connect through:

```text
mongodb://localhost:27017
```

Start and initialize the cluster:

```bash
docker compose up -d
bash scripts/init-cluster.sh
```

Check cluster status:

```bash
bash scripts/check-cluster.sh
```

The `ais` database is enabled for sharding. The `ais.raw_positions` collection is sharded by:

```javascript
{ MMSI: "hashed" }
```

## Manual Test

Connect to the cluster:

```bash
docker exec -it mongos mongosh --port 27017
```

Insert a test document:

```javascript
use ais
db.raw_positions.insertOne({
  MMSI: 123456789,
  Latitude: 55.7,
  Longitude: 12.6,
  timestamp: new Date()
})

db.raw_positions.find()
```

## Download Dataset

The dataset is not committed to Git because it is large. Download and extract it locally with:

```bash
bash scripts/download-data.sh
```

Expected output file:

```text
data/aisdk-2026-04-18.csv
```

## Task 2: Parallel Data Insertion

Install Python dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Import a safe first sample of 100,000 AIS records:

```bash
python scripts/01_import_parallel.py --drop --max-rows 100000 --workers 4
```

Import more records after the first test:

```bash
python scripts/01_import_parallel.py --drop --max-rows 500000 --workers 4
```

Use `--max-rows 0` only if the machine has enough memory and disk space for the full CSV:

```bash
python scripts/01_import_parallel.py --drop --max-rows 0 --workers 4
```

The importer reads the CSV in batches and uses a separate `MongoClient` inside each parallel insert task.

When `--drop` is used, the importer drops `ais.raw_positions`, recreates it as a sharded collection using:

```javascript
{ MMSI: "hashed" }
```

and then imports the CSV data.
