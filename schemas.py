# schemas.py

ride_request_schema = {
    "namespace": "ridehailing.avro",
    "type": "record",
    "name": "RideRequest",
    "fields": [
        {"name": "request_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "pickup_location", "type": {
            "type": "record", "name": "RideLocation",
            "fields": [{"name": "latitude", "type": "double"}, {"name": "longitude", "type": "double"}]
        }},
        {"name": "destination", "type": "RideLocation"},
        {"name": "status", "type": {
            "type": "enum", "name": "RequestStatus",
            "symbols": ["REQUESTED", "ACCEPTED", "CANCELLED", "COMPLETED"]
        }},
        {"name": "vehicle_type", "type": "string"},
        {"name": "estimated_fare", "type": "double"},
        {"name": "estimated_duration", "type": "int"},
        {"name": "estimated_distance", "type": "double"},
        {"name": "passenger_count", "type": "int"}
    ]
}

driver_status_schema = {
    "namespace": "ridehailing.avro",
    "type": "record",
    "name": "DriverStatusRecord",  # 👈 changed from DriverStatus
    "fields": [
        {"name": "driver_id", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "current_location", "type": {
            "type": "record", "name": "DriverStatusLocation",  # 👈 changed
            "fields": [
                {"name": "latitude", "type": "double"},
                {"name": "longitude", "type": "double"}
            ]
        }},
        {"name": "status", "type": {
            "type": "enum", "name": "DriverStatusEnumUnique",  # 👈 changed
            "symbols": ["AVAILABLE", "BUSY", "OFFLINE", "EN_ROUTE_TO_PICKUP", "WITH_PASSENGER"]
        }},
        {"name": "vehicle_info", "type": {
            "type": "record", "name": "DriverStatusVehicle",  # 👈 changed
            "fields": [
                {"name": "vehicle_id", "type": "string"},
                {"name": "vehicle_type", "type": "string"},
                {"name": "capacity", "type": "int"}
            ]
        }},
        {"name": "current_ride_id", "type": ["null", "string"]},
        {"name": "last_update", "type": "long"},
        {"name": "battery_level", "type": ["null", "double"]}
    ]
}

