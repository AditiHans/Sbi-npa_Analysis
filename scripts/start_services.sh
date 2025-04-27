#!/bin/bash
# Start Zookeeper
./kafka_2.13-3.3.1/bin/zookeeper-server-start.sh config/zookeeper.properties &

# Start Kafka
./kafka_2.13-3.3.1/bin/kafka-server-start.sh config/server.properties &

# Start NiFi
./nifi-1.15.0/bin/nifi.sh start