#!/usr/bin/env bash

echo "Starting Zookeeper"
docker run -d --name zookeeper -p 2181:2181 confluent/zookeeper

echo "Starting Kafka"
docker run -d --name kafka -p 9092:9092 --link zookeeper:zookeeper --env KAFKA_ADVERTISED_HOST_NAME=localhost confluent/kafka

echo "Kafka broker started, hit CTRL+C to exit"
trap ctrl_c INT

function ctrl_c() {
  echo "Stopping Kafka and Zookeeper"
  docker stop kafka
  docker stop zookeeper
  docker rm -f kafka
  docker rm -f zookeeper
  exit 0
}

# Wait forever
read -r -d '' _ </dev/tty

