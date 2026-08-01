import json

import random

import time

import logging

from datetime import datetime

from kafka import KafkaProducer

from kafka.errors import KafkaError


# Logging configuration

logging.basicConfig(

    filename="air_quality.log",

    level=logging.INFO,

    format="%(asctime)s - %(levelname)s - %(message)s"

)


# Kafka producer with at-least-once style configuration

producer = KafkaProducer(

    bootstrap_servers="localhost:9092",

    acks="all",

    retries=5,

    max_in_flight_requests_per_connection=1,

    request_timeout_ms=30000,

    retry_backoff_ms=1000,

    value_serializer=lambda v: json.dumps(v).encode("utf-8")

)


zones = ["North", "South", "East", "West", "Central"]


print("Starting Air Quality Producer with retry logic. Press Ctrl+C to stop.")


while True:

    try:

        # Base AQI value

        aqi = random.randint(20, 400)


        # Simulate 5% out-of-range AQI values for DLQ testing

        if random.random() < 0.05:

            aqi = random.choice([-10, 999])

            logging.warning("Out-of-range AQI generated for DLQ testing.")


        # Simulate 5% null AQI values as required by the assignment

        if random.random() < 0.05:

            aqi = None

            logging.warning("Null AQI generated and handled gracefully.")


        event = {

            "sensor_id": f"AQ{random.randint(100, 999)}",

            "zone": random.choice(zones),

            "pm25": round(random.uniform(10, 250), 2),

            "pm10": round(random.uniform(20, 300), 2),

            "no2": round(random.uniform(5, 150), 2),

            "aqi": aqi,

            "timestamp": datetime.utcnow().isoformat()

        }


        max_attempts = 3

        attempt = 0

        sent = False


        while attempt < max_attempts and not sent:

            try:

                attempt += 1


                # Simulate occasional sensor/network timeout before send attempt

                if random.random() < 0.03:

                    raise TimeoutError("Simulated air quality sensor timeout")


                future = producer.send(

                    "urbanpulse.air_quality",

                    value=event

                )


                metadata = future.get(timeout=10)


                logging.info(

                    f"Sent air_quality event | topic={metadata.topic} | "

                    f"partition={metadata.partition} | offset={metadata.offset} | "

                    f"attempt={attempt}"

                )


                print(event)

                sent = True


            except (KafkaError, TimeoutError) as e:

                logging.warning(

                    f"Send attempt {attempt} failed for air_quality event. Reason: {e}"

                )


                if attempt < max_attempts:

                    logging.info("Retrying air_quality event after short backoff.")

                    time.sleep(2)

                else:

                    logging.error(

                        f"Failed to send air_quality event after {max_attempts} attempts: {event}"

                    )


        time.sleep(1)


    except KeyboardInterrupt:

        print("Stopping Air Quality Producer.")

        logging.info("Air Quality Producer stopped by user.")

        break


    except Exception as e:

        logging.error(f"Unexpected error in air_quality_producer.py: {e}")

        time.sleep(2)
