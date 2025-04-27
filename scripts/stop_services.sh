#!/bin/bash
# Stop NiFi
./nifi-1.15.0/bin/nifi.sh stop

# Stop Kafka
./kafka_2.13-3.3.1/bin/kafka-server-stop.sh

# Stop Zookeeper
./kafka_2.13-3.3.1/bin/zookeeper-server-stop.sh