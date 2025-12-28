import sqlite3
import requests
import json
from datetime import datetime

def check_weather_data():
    db_path = 'weather.db'
    
    print(f"--- DATABASE CHECK: {db_path} ---")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check locations and latest observation
        query = """
            SELECT l.city_name, cw.temperature, cw.observation_time, l.latitude, l.longitude
            FROM locations l
            JOIN current_weather cw ON l.location_id = cw.location_id
            LIMIT 5
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if not rows:
            print("No weather data found in current_weather table.")
            return

        for city, temp, obs_time, lat, lon in rows:
            print(f"City: {city}")
            print(f"  DB Temperature: {temp}°C")
            print(f"  Last Observation: {obs_time}")
            
            # Now fetch LIVE data from Open-Meteo for the same coordinates to compare
            live_api_url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m",
                "timezone": "auto"
            }
            try:
                res = requests.get(live_api_url, params=params, timeout=10)
                res.raise_for_status()
                live_data = res.json()
                live_temp = live_data['current']['temperature_2m']
                print(f"  Live Open-Meteo: {live_temp}°C")
                diff = abs(temp - live_temp)
                if diff < 2:
                    print(f"  Status: VERIFIED (Diff: {diff:.2f}°C)")
                else:
                    print(f"  Status: VARIANCE (Diff: {diff:.2f}°C) - Data might be cached or from a different hour.")
            except Exception as e:
                print(f"  Live Sync Error: {e}")
            print("-" * 30)
            
        conn.close()
    except Exception as e:
        print(f"Database Error: {e}")

if __name__ == "__main__":
    check_weather_data()
