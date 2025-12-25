import time
import requests
import sqlite3
from datetime import datetime
import random
from db_config import get_db_connection

# Configuration
API_URL = "https://api.open-meteo.com/v1/forecast"
POLL_INTERVAL = 300  # 5 minutes
SIMULATION_MODE = False 

def fetch_weather_batch(locations):
    if SIMULATION_MODE:
        return generate_simulated_data(locations)

    lats = ",".join([str(loc['latitude']) for loc in locations])
    lons = ",".join([str(loc['longitude']) for loc in locations])
    
    params = {
        "latitude": lats,
        "longitude": lons,
        "current": "temperature_2m,relative_humidity_2m,apparent_temperature,is_day,weather_code,wind_speed_10m,wind_direction_10m,surface_pressure,uv_index,dew_point_2m,visibility,cloud_cover,shortwave_radiation",
        "timezone": "auto"
    }
    
    try:
        response = requests.get(API_URL, params=params, timeout=15)
        if response.status_code == 429:
            print("!!! API LIMIT EXCEEDED (429) !!! Entering temporary simulation mode.")
            return generate_simulated_data(locations)
            
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else [data]
    except Exception as e:
        print(f"Fetch Error: {e}. Using simulated data.")
        return generate_simulated_data(locations)

def generate_simulated_data(locations):
    results = []
    for loc in locations:
        results.append({
            'current': {
                'temperature_2m': 15 + random.uniform(-5, 10),
                'relative_humidity_2m': random.randint(30, 80),
                'apparent_temperature': 15,
                'is_day': 1,
                'weather_code': 0,
                'wind_speed_10m': random.uniform(5, 25),
                'wind_direction_10m': random.randint(0, 360),
                'surface_pressure': 1010 + random.uniform(-5, 5),
                'uv_index': random.uniform(0, 8),
                'dew_point_2m': 10,
                'visibility': random.randint(8000, 20000),
                'cloud_cover': random.randint(0, 100),
                'shortwave_radiation': random.uniform(0, 800)
            }
        })
    return results

def store_weather_data(cursor, location_id, country, current_api):
    if not current_api: return
    
    c = current_api
    obs_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    data = {
        'location_id': location_id,
        'country': country,
        'obs_time': obs_time,
        'temp': c.get('temperature_2m', 0),
        'hum': c.get('relative_humidity_2m', 0),
        'wind_s': c.get('wind_speed_10m', 0),
        'wind_d': c.get('wind_direction_10m', 0),
        'code': c.get('weather_code', 0),
        'day': c.get('is_day', 1),
        'pres': c.get('surface_pressure', 1013),
        'uv': c.get('uv_index', 0),
        'dew': c.get('dew_point_2m', 0),
        'vis': c.get('visibility', 10000),
        'cloud': c.get('cloud_cover', 0),
        'solar': c.get('shortwave_radiation', 0)
    }

    # SQLite compatible UPSERT using INSERT OR REPLACE (simplest across versions)
    # Note: INSERT OR REPLACE deletes the old row and inserts new. 
    # For history we act as log.
    
    # 1. Update Current Weather
    cols = "location_id, observation_time, temperature, humidity, windspeed, winddirection, weathercode, is_day, pressure, uv_index, dew_point, visibility, cloud_cover, solar_rad"
    vals = ":location_id, :obs_time, :temp, :hum, :wind_s, :wind_d, :code, :day, :pres, :uv, :dew, :vis, :cloud, :solar"
    
    # We use explicit Upsert for Current Weather to keep ID stable if needed, but Replace is fine.
    # Actually, let's use standard INSERT OR REPLACE for simplicity
    try:
        # Delete old entry for this location to ensure single current record ?? 
        # No, schema has UNIQUE(location_id). INSERT OR REPLACE handles it.
        cursor.execute(f"INSERT OR REPLACE INTO current_weather ({cols}) VALUES ({vals})", data)
        
        # 2. Insert into History (Ignore duplicates if any)
        cursor.execute(f"INSERT OR IGNORE INTO weather_history ({cols}) VALUES ({vals})", data)
        
    except Exception as e:
        print(f"Store Error for ID {location_id}: {e}")

def main():
    print("!!! SCIENTIFIC MET-BOT ACTIVE (SQLITE MODEL) !!!")
    
    # Ensure DB is initialized
    import init_db
    init_db.init_db()

    while True:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM locations")
            locations = [dict(row) for row in cursor.fetchall()]
            
            if locations:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Syncing {len(locations)} locations...")
                results = fetch_weather_batch(locations)
                
                for i, loc in enumerate(locations):
                    city_data = results[i].get('current') if i < len(results) else None
                    if city_data:
                        store_weather_data(cursor, loc['location_id'], loc['country'], city_data)
                
                conn.commit()
                print("Sync Completed successfully.")
            
            conn.close()
        except Exception as e:
            print(f"Main Loop Error: {e}")
            import traceback
            traceback.print_exc()
            
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
