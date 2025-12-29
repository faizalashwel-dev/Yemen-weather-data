
import requests
from weather_fetcher import fetch_weather_batch

locations = [
    {'latitude': 15.3694, 'longitude': 44.1910, 'city_name': 'Sana\'a'},
    {'latitude': 12.7794, 'longitude': 45.0367, 'city_name': 'Aden'}
]

print("Testing Open-Meteo API...")
results = fetch_weather_batch(locations)
print("Results type:", type(results))
if isinstance(results, list):
    print("Count:", len(results))
    for i, res in enumerate(results):
        print(f"Location {i}:", res.get('current'))
        # Check if it looks simulated (randomized function in weather_fetcher uses base 15 +/-)
        # Real data should be specific.
else:
    print("Result:", results)
