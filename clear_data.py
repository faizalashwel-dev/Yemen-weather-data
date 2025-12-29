
import sqlite3

DB_FILE = 'weather.db'

def clear_weather_data():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        print("Clearing current_weather table...")
        cursor.execute("DELETE FROM current_weather")
        
        print("Clearing weather_history table...")
        cursor.execute("DELETE FROM weather_history")
        
        conn.commit()
        print("Weather data cleared. The system will now only contain new, real data when the fetcher runs.")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    clear_weather_data()
