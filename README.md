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
{ MMSI: 1 }
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
