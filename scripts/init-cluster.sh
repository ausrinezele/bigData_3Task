#!/usr/bin/env bash
set -euo pipefail

echo "Starting MongoDB containers..."
docker compose up -d

echo "Waiting for MongoDB containers to accept connections..."
sleep 8

echo "Initializing config server replica set..."
docker exec configsvr1 mongosh --port 27019 --quiet --eval '
try {
  rs.status();
  print("configReplSet already initialized");
} catch (e) {
  rs.initiate({
    _id: "configReplSet",
    configsvr: true,
    members: [
      { _id: 0, host: "configsvr1:27019" }
    ]
  });
  print("configReplSet initialized");
}
'

echo "Initializing shard 1 replica set..."
docker exec shard1a mongosh --port 27018 --quiet --eval '
try {
  rs.status();
  print("shard1ReplSet already initialized");
} catch (e) {
  rs.initiate({
    _id: "shard1ReplSet",
    members: [
      { _id: 0, host: "shard1a:27018" },
      { _id: 1, host: "shard1b:27018" }
    ]
  });
  print("shard1ReplSet initialized");
}
'

echo "Initializing shard 2 replica set..."
docker exec shard2a mongosh --port 27018 --quiet --eval '
try {
  rs.status();
  print("shard2ReplSet already initialized");
} catch (e) {
  rs.initiate({
    _id: "shard2ReplSet",
    members: [
      { _id: 0, host: "shard2a:27018" },
      { _id: 1, host: "shard2b:27018" }
    ]
  });
  print("shard2ReplSet initialized");
}
'

echo "Waiting for replica sets to elect primary nodes..."
sleep 10

echo "Adding shards to mongos router..."
docker exec mongos mongosh --port 27017 --quiet --eval '
const existingShards = db.adminCommand({ listShards: 1 }).shards.map((shard) => shard._id);

if (!existingShards.includes("shard1ReplSet")) {
  sh.addShard("shard1ReplSet/shard1a:27018,shard1b:27018");
  print("Added shard1ReplSet");
} else {
  print("shard1ReplSet already added");
}

if (!existingShards.includes("shard2ReplSet")) {
  sh.addShard("shard2ReplSet/shard2a:27018,shard2b:27018");
  print("Added shard2ReplSet");
} else {
  print("shard2ReplSet already added");
}
'

echo "Creating AIS database, index, and sharded collection..."
docker exec mongos mongosh --port 27017 --quiet --eval '
sh.enableSharding("ais");

const dbName = db.getSiblingDB("ais");
dbName.raw_positions.createIndex({ MMSI: "hashed" });

try {
  sh.shardCollection("ais.raw_positions", { MMSI: "hashed" });
  print("ais.raw_positions sharded by MMSI");
} catch (e) {
  if (String(e).includes("already sharded")) {
    print("ais.raw_positions already sharded");
  } else {
    throw e;
  }
}
'
echo "MongoDB sharded cluster is ready at mongodb://localhost:27017"
