#!/usr/bin/env bash
set -euo pipefail

echo "Docker containers:"
docker ps --filter "name=configsvr1" \
  --filter "name=shard1a" \
  --filter "name=shard1b" \
  --filter "name=shard2a" \
  --filter "name=shard2b" \
  --filter "name=mongos" \
  --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo
echo "MongoDB sharding status:"
docker exec mongos mongosh --port 27017 --quiet --eval '
sh.status();
'

echo
echo "AIS collections:"
docker exec mongos mongosh --port 27017 --quiet --eval '
const ais = db.getSiblingDB("ais");
printjson(ais.getCollectionNames());
print("raw_positions documents: " + ais.raw_positions.countDocuments());
'
