#!/bin/bash


BOOTSTRAP_SERVER="localhost:9092"


kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER --entity-type topics --entity-name urbanpulse.bus_gps --alter --add-config retention.ms=86400000


kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER --entity-type topics --entity-name urbanpulse.air_quality --alter --add-config retention.ms=7776000000


kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER --entity-type topics --entity-name urbanpulse.smart_meters --alter --add-config retention.ms=31536000000


kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER --entity-type topics --entity-name urbanpulse.traffic_signals --alter --add-config retention.ms=604800000


kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER --entity-type topics --entity-name urbanpulse.dlq --alter --add-config retention.ms=2592000000
