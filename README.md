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

The importer reads the CSV in batches and uses a separate `MongoClient` inside each parallel insert task.

When `--drop` is used, the importer drops `ais.raw_positions`, recreates it as a sharded collection using:

```javascript
{ MMSI: "hashed" }
```

and then imports the CSV data.

## Task 3: Parallel Noise Filtering

Filter valid AIS records into a separate sharded collection:

```bash
python scripts/02_filter_noise_parallel.py --drop --workers 4
```

The filtering script removes records with missing or invalid values for:

- `Navigational status`
- `MMSI`
- `Latitude`
- `Longitude`
- `ROT`
- `SOG`
- `COG`
- `Heading`
- `timestamp`

It also keeps only vessels with at least 100 valid data points and writes the result to:

```text
ais.filtered_positions
```

## Task 4: Calculation of Delta t and Histogram Generation

After filtering the noisy AIS records, delta t is calculated from the MongoDB collection `ais.filtered_positions`.

For each vessel, records are grouped by `MMSI` and sorted by `timestamp`. The time difference between two subsequent data points for the same vessel is calculated in milliseconds:

`delta_t_ms = current timestamp - previous timestamp`

The calculated delta t values are inserted into the MongoDB collection `ais.delta_t_positions`.

Run the script:

`python scripts/03_calculate_delta_t_histogram.py --drop --workers 4`

The script generates these files:

- `outputs/delta_t_values.csv`
- `outputs/delta_t_histogram.png`
- `outputs/delta_t_summary.txt`

In our run, the script processed 219 vessels and inserted 11,277 delta t documents into `ais.delta_t_positions`.

Summary statistics:

- Delta t count: 11,277
- Minimum: 1,000 ms
- Maximum: 98,000 ms
- Mean: 8,056.31 ms
- Median: 9,000 ms
- 25th percentile: 6,000 ms
- 75th percentile: 10,000 ms
- 95th percentile: 11,000 ms

### Histogram Analysis

The histogram shows that most AIS messages in the filtered dataset are received between approximately 6,000 ms and 10,000 ms. The median value is 9,000 ms, which means that a typical vessel update happens about every 9 seconds.

The 95th percentile is 11,000 ms, meaning that 95% of all calculated delta t values are 11 seconds or less. This shows that most vessels report their AIS positions frequently and consistently.

A few larger delta t values are also present, with the maximum value reaching 98,000 ms. These larger gaps may be caused by missing AIS messages, temporary loss of receiver coverage, vessels leaving or entering coverage, or changes in reporting behavior.

Overall, the histogram suggests that most filtered vessels have stable AIS reporting intervals, while only a small number of records show longer communication gaps.
