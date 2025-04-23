from azure.eventhub import EventHubProducerClient, EventData
import fastavro
import io
import time
from schemas import ride_request_schema, driver_status_schema
from ride_hailing_generator import RideHailingDataGenerator
import datetime
import random
# Connection strings (Update with your own keys)
ride_requests_conn_str = "Endpoint=sb://iesstsabbadbaa-grp-06-10.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=XimkJFhlc8Gvorlhh7PRqEZVj0gDkiuDk+AEhH8O9is=;EntityPath=ride_request_9"
driver_status_conn_str = "Endpoint=sb://iesstsabbadbaa-grp-06-10.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=XimkJFhlc8Gvorlhh7PRqEZVj0gDkiuDk+AEhH8O9is=;EntityPath=driver_status_9"
# AVRO serializer
def serialize_avro(record, schema):
    out = io.BytesIO()
    fastavro.schemaless_writer(out, schema, record)
    return out.getvalue()

# Parse AVRO schemas
parsed_ride_schema = fastavro.parse_schema(ride_request_schema)
parsed_driver_schema = fastavro.parse_schema(driver_status_schema)

# Simulated Data Generator
config = {
    'num_drivers': 10,
    'num_users': 50,
    'base_request_rate': 5
}
generator = RideHailingDataGenerator(config)

# EventHub producers
producer_ride = EventHubProducerClient.from_connection_string(ride_requests_conn_str)
producer_driver = EventHubProducerClient.from_connection_string(driver_status_conn_str)

while True:
    ride_requests = generator.generate_ride_requests()

    # Send ride_requests data
    ride_batch = producer_ride.create_batch()
    for req in ride_requests:
        ride_batch.add(EventData(serialize_avro(req, parsed_ride_schema)))
    producer_ride.send_batch(ride_batch)

    # Send driver_status data with slight timestamp variation
    madrid_now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=2)))
    base_driver_ts = int(madrid_now.timestamp())
    driver_batch = producer_driver.create_batch()
    for driver in generator.drivers.values():
        driver["timestamp"] = base_driver_ts + random.randint(0, 4)
        driver_batch.add(EventData(serialize_avro(driver, parsed_driver_schema)))
    producer_driver.send_batch(driver_batch)

    print(f"Sent {len(ride_requests)} ride requests and {len(generator.drivers)} driver statuses.")
    time.sleep(5)

