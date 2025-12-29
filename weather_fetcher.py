import time
import requests
import sqlite3
import urllib.parse
from datetime import datetime
from db_config import get_db_connection

# Configuration
POLL_INTERVAL = 300  # 5 minutes

def fetch_wttr(city_name):
    try:
        # Sanitize city name for URL (wttr.in handles space as + or %20)
        # wttr.in usually prefers + for spaces
        safe_name = city_name.replace("'", "").replace(" ", "+")
        if city_name == "Sa'dah": safe_name = "Sa'dah" # Special handling if needed, but quote usually works
        
        # Best approach: Quote everything
        # Actually wttr.in/Sana'a works. wttr.in/Sana%27a works.
        encoded_name = urllib.parse.quote(city_name)
        
        url = f"https://wttr.in/{encoded_name}?format=j1"
        
        # User-Agent to avoid blocking
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            print(f"Failed to fetch {city_name}: {response.status_code}")
            return None
            
        data = response.json()
        if 'current_condition' not in data:
            return None
            
        current = data['current_condition'][0]
        
        # Parse fields
        return {
            'temp': float(current['temp_C']),
            'hum': float(current['humidity']),
            'wind_s': float(current['windspeedKmph']),
            'wind_d': float(current['winddirDegree']),
            'code': int(current['weatherCode']), 
            'pres': float(current['pressure']),
            'uv': float(current['uvIndex']),
            'vis': float(current['visibility']) * 1000, # km to m
            'cloud': float(current['cloudcover']),
            # wttr.in misses these sometimes, use defaults
            'dew': 0.0, 
            'solar': 0.0,
            'day': 1 # Assume day if not provided, or logic later
        }
    except Exception as e:
        print(f"Error fetching {city_name}: {e}")
        return None

def main():
    print("!!! SCIENTIFIC MET-BOT ACTIVE (WTTR.IN REALTIME SOURCE) !!!")
    
    # Ensure DB is initialized
    try:
        import init_db
        init_db.init_db()
    except:
        pass

    while True:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM locations")
            locations = [dict(row) for row in cursor.fetchall()]
            
            if locations:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Syncing {len(locations)} locations via WTTR.IN...")
                
                for loc in locations:
                    city_data = fetch_wttr(loc['city_name'])
                    if city_data:
                        obs_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        cols = "location_id, observation_time, temperature, humidity, windspeed, winddirection, weathercode, pressure, uv_index, visibility, cloud_cover, dew_point, solar_rad, is_day"
                        vals = "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
                        
                        params = (
                            loc['location_id'], obs_time, 
                            city_data['temp'], city_data['hum'], 
                            city_data['wind_s'], city_data['wind_d'], 
                            city_data['code'], city_data['pres'], 
                            city_data['uv'], city_data['vis'], 
                            city_data['cloud'], city_data['dew'],
                            city_data['solar'], city_data['day']
                        )
                        
                        # UPSERT Current
                        cursor.execute(f"INSERT OR REPLACE INTO current_weather ({cols}) VALUES ({vals})", params)
                        
                        # Insert History
                        cursor.execute(f"INSERT OR IGNORE INTO weather_history ({cols}) VALUES ({vals})", params)
                        
                        print(f" > {loc['city_name']}: {city_data['temp']}°C")
                    
                    time.sleep(1) # Rate limit protection
                
                conn.commit()
                print("Sync Completed successfully.")
            
            conn.close()
        except Exception as e:
            print(f"Main Loop Error: {e}")
            
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
