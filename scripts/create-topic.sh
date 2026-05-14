#!/bin/bash
# Tạo topic appointment-events trong Redpanda
docker exec -it redpanda rpk topic create appointment-events --partitions 1 --replicas 1
echo "Da tao topic appointment-events"
