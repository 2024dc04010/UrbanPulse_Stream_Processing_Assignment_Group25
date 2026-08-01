#!/bin/bash


BOOTSTRAP_SERVER="localhost:9092"


kafka-topics.sh --create --topic urbanpulse.bus_gps --partitions 12 --replication-factor 1 --bootstrap-server $BOOTSTRAP_SERVER


kafka-topics.sh --create --topic urbanpulse.air_quality --partitions 3 --replication-factor 1 --bootstrap-server $BOOTSTRAP_SERVER


kafka-topics.sh --create --topic urbanpulse.traffic_signals --partitions 6 --replication-factor 1 --bootstrap-server $BOOTSTRAP_SERVER


kafka-topics.sh --create --topic urbanpulse.smart_meters --partitions 8 --replication-factor 1 --bootstrap-server $BOOTSTRAP_SERVER


kafka-topics.sh --create --topic urbanpulse.dlq --partitions 1 --replication-factor 1 --bootstrap-server $BOOTSTRAP_SERVER
 
