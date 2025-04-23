import numpy as np
import random
import math


class CityGrid:
    def __init__(self, lat_range=(40.70, 40.85), lon_range=(-74.05, -73.90)):
        """
        Initialize a city grid with hotspots
        Default coordinates approximate New York City
        """
        self.lat_range = lat_range
        self.lon_range = lon_range
        
        # Define hotspots (business districts, residential areas, entertainment zones)
        self.hotspots = {
            'business_districts': [
                {'center': (40.75, -73.98), 'radius': 0.02, 'name': 'Midtown'}, # Midtown
                {'center': (40.71, -74.01), 'radius': 0.015, 'name': 'Financial District'}, # Financial District
            ],
            'residential_areas': [
                {'center': (40.78, -73.95), 'radius': 0.025, 'name': 'Upper East Side'}, # Upper East Side
                {'center': (40.73, -73.99), 'radius': 0.02, 'name': 'Greenwich Village'}, # Greenwich Village
                {'center': (40.80, -73.96), 'radius': 0.025, 'name': 'Upper West Side'}, # Upper West Side
            ],
            'entertainment_zones': [
                {'center': (40.76, -73.98), 'radius': 0.015, 'name': 'Times Square'}, # Times Square
                {'center': (40.74, -73.99), 'radius': 0.015, 'name': 'Chelsea'}, # Chelsea
            ],
            'airports': [
                {'center': (40.64, -73.78), 'radius': 0.03, 'name': 'JFK Airport'}, # JFK Airport
                {'center': (40.77, -73.87), 'radius': 0.02, 'name': 'LaGuardia Airport'}, # LaGuardia
            ]
        }
        
    def get_random_location(self, hotspot_bias=0.8):
        """
        Generate a random location within the city grid
        With a bias towards hotspots
        """
        if random.random() < hotspot_bias:
            # Select a random hotspot category
            category = random.choice(list(self.hotspots.keys()))
            # Select a random hotspot within that category
            hotspot = random.choice(self.hotspots[category])
            
            # Generate a point within the hotspot radius
            angle = random.uniform(0, 2 * math.pi)
            radius = random.uniform(0, hotspot['radius'])
            
            # Convert polar coordinates to lat/lon offset
            lat_offset = radius * math.cos(angle)
            lon_offset = radius * math.sin(angle)
            
            return (
                hotspot['center'][0] + lat_offset,
                hotspot['center'][1] + lon_offset
            )
        else:
            # Generate a completely random location within the city bounds
            return (
                random.uniform(self.lat_range[0], self.lat_range[1]),
                random.uniform(self.lon_range[0], self.lon_range[1])
            )
    
    def get_hotspot_demand_factor(self, location, time_of_day):
        """
        Calculate demand factor based on location and time of day
        Returns a multiplier for ride request probability
        """
        lat, lon = location
        demand_factor = 1.0
        
        # Check if location is in or near a hotspot
        for category, hotspots in self.hotspots.items():
            for hotspot in hotspots:
                distance = geodesic(
                    (lat, lon), 
                    hotspot['center']
                ).kilometers
                
                # If within hotspot radius, adjust demand factor
                if distance <= hotspot['radius'] * 111:  # Convert degrees to km approximately
                    if category == 'business_districts':
                        # Higher demand during morning and evening rush hours
                        if 7 <= time_of_day.hour < 10:  # Morning rush
                            demand_factor += 2.0
                        elif 16 <= time_of_day.hour < 19:  # Evening rush
                            demand_factor += 2.5
                        elif 12 <= time_of_day.hour < 14:  # Lunch time
                            demand_factor += 1.5
                        elif 22 <= time_of_day.hour or time_of_day.hour < 5:  # Late night
                            demand_factor += 0.2
                    
                    elif category == 'residential_areas':
                        # Higher demand in early morning and evening
                        if 6 <= time_of_day.hour < 9:  # Early morning
                            demand_factor += 1.8
                        elif 17 <= time_of_day.hour < 20:  # Evening return
                            demand_factor += 1.2
                    
                    elif category == 'entertainment_zones':
                        # Higher demand in evenings and late night
                        if 18 <= time_of_day.hour < 22:  # Evening
                            demand_factor += 2.0
                        elif 22 <= time_of_day.hour or time_of_day.hour < 3:  # Late night
                            demand_factor += 3.0
                    
                    elif category == 'airports':
                        # Steady demand throughout day with slight peaks
                        demand_factor += 1.0
                        if 7 <= time_of_day.hour < 10 or 16 <= time_of_day.hour < 20:
                            demand_factor += 0.5
        
        return max(0.1, demand_factor)  # Ensure minimum demand factor

    def calculate_trip_details(self, pickup, destination):
        """
        Calculate estimated trip duration, distance and fare
        """
        # Calculate distance in kilometers
        distance_km = geodesic(pickup, destination).kilometers
        
        # Estimate duration (assume average speed of 25 km/h in city)
        # Add random variation to account for traffic
        avg_speed = random.uniform(15, 35)  # km/h
        duration_minutes = (distance_km / avg_speed) * 60
        
        # Add random traffic delay (0-10 minutes)
        duration_minutes += random.uniform(0, 10)
        
        # Calculate fare
        base_fare = 2.50
        per_km_rate = 1.75
        per_minute_rate = 0.35
        
        fare = base_fare + (distance_km * per_km_rate) + (duration_minutes * per_minute_rate)
        
        # Round values
        distance_km = round(distance_km, 2)
        duration_minutes = int(round(duration_minutes))
        fare = round(fare, 2)
        
        return {
            'distance': distance_km,
            'duration': duration_minutes,
            'fare': fare
        }

# Test the CityGrid class
if __name__ == "__main__":
    city = CityGrid()
    test_location = city.get_random_location()
    print(f"Random location: {test_location}")
    
    test_time = datetime.datetime.now()
    demand = city.get_hotspot_demand_factor(test_location, test_time)
    print(f"Demand factor at {test_time.hour}:00: {demand}")
    
    pickup = city.get_random_location()
    destination = city.get_random_location()
    trip = city.calculate_trip_details(pickup, destination)
    print(f"Trip from {pickup} to {destination}:")
    print(f"  Distance: {trip['distance']} km")
    print(f"  Duration: {trip['duration']} minutes")
    print(f"  Fare: ${trip['fare']}")