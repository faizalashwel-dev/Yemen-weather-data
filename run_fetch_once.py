
import time
import sqlite3
from datetime import datetime
from db_config import get_db_connection
from weather_fetcher import fetch_wttr
import init_db

def run_once():
    print("Initiating ONE-TIME weather fetch (WTTR.IN)...")
    init_db.init_db()
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM locations")
        locations = [dict(row) for row in cursor.fetchall()]
        
        if locations:
            print(f"Syncing {len(locations)} locations...")
            
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
                    
                    cursor.execute(f"INSERT OR REPLACE INTO current_weather ({cols}) VALUES ({vals})", params)
                    cursor.execute(f"INSERT OR IGNORE INTO weather_history ({cols}) VALUES ({vals})", params)
                    print(f" > {loc['city_name']}: {city_data['temp']}°C Updated")
                
                time.sleep(0.5)
            
            conn.commit()
            print("Sync Completed successfully.")
        
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    run_once()
