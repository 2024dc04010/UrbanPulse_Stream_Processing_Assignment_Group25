# UrbanPulse

DSE ZG556 - Stream Processing and Analytics

## Task B Completed

### Kafka Topics

- urbanpulse.bus_gps
- urbanpulse.air_quality
- urbanpulse.traffic_signals
- urbanpulse.smart_meters
- urbanpulse.dlq

### Retention

- bus_gps: 24 hours
- air_quality: 90 days
- smart_meters: 365 days

### Producers

- bus_gps_producer.py
- air_quality_producer.py

### DLQ

- dlq_processor.py
- dlq_report.py

Validation rules:
- NULL_AQI
- AQI_OUT_OF_RANGE
- INVALID_GPS_COORDINATES
