import random
import uuid
import datetime
import json
from faker import Faker
from city_grid import CityGrid  # Make sure this exists as a separate file or include its code too


# Step 3: Implement the core RideHailingDataGenerator class

class RideHailingDataGenerator:
    def __init__(self, config):
        self.config = config
        self.faker = Faker()
        self.city_grid = CityGrid()

        # Use current Madrid time as the start time
        madrid_tz = datetime.timezone(datetime.timedelta(hours=2))  # UTC+2 for CEST
        self.current_time = datetime.datetime.now(tz=madrid_tz).replace(microsecond=0)

        self.vehicle_types = {
            'Economy': 4,
            'Comfort': 4,
            'Premium': 4,
            'SUV': 6,
            'Van': 8
        }

        self.drivers = self._initialize_drivers(config['num_drivers'])
        self.users = self._initialize_users(config['num_users'])

        self.active_rides = {}

        print(f"Initialized RideHailingDataGenerator with {len(self.drivers)} drivers and {len(self.users)} users")

    def _initialize_drivers(self, num_drivers):
        drivers = {}

        for _ in range(num_drivers):
            driver_id = f"D-{str(uuid.uuid4())[:8]}"
            location = self.city_grid.get_random_location()
            vehicle_type = random.choice(list(self.vehicle_types.keys()))
            status = random.choices(['AVAILABLE', 'BUSY', 'OFFLINE'], weights=[0.6, 0.3, 0.1])[0]

            drivers[driver_id] = {
                'driver_id': driver_id,
                'current_location': {
                    'latitude': location[0],
                    'longitude': location[1]
                },
                'status': status,
                'vehicle_info': {
                    'vehicle_id': f"V-{str(uuid.uuid4())[:8]}",
                    'vehicle_type': vehicle_type,
                    'capacity': self.vehicle_types[vehicle_type]
                },
                'current_ride_id': None,
                'last_update': int(self.current_time.timestamp()),
                'battery_level': random.uniform(0.3, 1.0)
            }

        return drivers

    def _initialize_users(self, num_users):
        users = {}

        for _ in range(num_users):
            user_id = f"U-{str(uuid.uuid4())[:8]}"
            home_location = self.city_grid.get_random_location(hotspot_bias=0.9)
            work_location = self.city_grid.get_random_location(hotspot_bias=0.9)
            preferred_vehicle_type = random.choices(
                list(self.vehicle_types.keys()), 
                weights=[0.5, 0.25, 0.15, 0.07, 0.03]
            )[0]

            users[user_id] = {
                'user_id': user_id,
                'name': self.faker.name(),
                'home_location': home_location,
                'work_location': work_location,
                'preferred_vehicle_type': preferred_vehicle_type,
                'cancellation_probability': random.uniform(0.02, 0.08),
                'last_activity': int(self.current_time.timestamp())
            }

        return users

    def generate_ride_requests(self):
        ride_requests = []
        num_requests = max(1, int(random.gauss(self.config['base_request_rate'], 1)))

        for _ in range(num_requests):
            user = random.choice(list(self.users.values()))
            request_id = f"R-{str(uuid.uuid4())[:8]}"

            pickup_location = {
                "latitude": user['home_location'][0],
                "longitude": user['home_location'][1]
            }

            destination = {
                "latitude": user['work_location'][0],
                "longitude": user['work_location'][1]
            }

            ride_requests.append({
                "request_id": request_id,
                "user_id": user["user_id"],
                "timestamp": int((self.current_time + datetime.timedelta(seconds=random.randint(0, 4))).timestamp()),
                "pickup_location": pickup_location,
                "destination": destination,
                "status": "REQUESTED",
                "vehicle_type": user["preferred_vehicle_type"],
                "estimated_fare": round(random.uniform(5.0, 50.0), 2),
                "estimated_duration": random.randint(5, 45),
                "estimated_distance": round(random.uniform(1.0, 20.0), 2),
                "passenger_count": random.randint(1, 4)
            })

        self.current_time += datetime.timedelta(seconds=5)

        return ride_requests

# Test the RideHailingDataGenerator initialization
if __name__ == "__main__":
    config = {
        'num_drivers': 10,
        'num_users': 50,
        'base_request_rate': 5
    }

    generator = RideHailingDataGenerator(config)

    sample_driver_id = list(generator.drivers.keys())[0]
    print("\nSample Driver:")
    print(json.dumps(generator.drivers[sample_driver_id], indent=2))

    sample_user_id = list(generator.users.keys())[0]
    print("\nSample User:")
    print(json.dumps(generator.users[sample_user_id], indent=2))
