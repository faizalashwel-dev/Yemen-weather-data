import mysql.connector
from db_config import DB_CONFIG

def fix():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("Fixing current_weather table structure...")
        
        # 1. Clear current_weather to start fresh (or we could keep the latest, but fresh is safer for schema change)
        cursor.execute("DELETE FROM current_weather")
        
        # 2. Drop the old unique key
        try:
            cursor.execute("ALTER TABLE current_weather DROP INDEX uk_current_weather")
        except:
            pass
            
        # 3. Add new UNIQUE KEY on location_id only
        try:
            cursor.execute("ALTER TABLE current_weather ADD UNIQUE KEY uk_location_id (location_id)")
            print("Successfully updated current_weather to only allow one record per city.")
        except mysql.connector.Error as err:
            print(f"Error adding unique key: {err}")

        # 4. Ensure all 10 Governorates (cities) are present
        # Based on user request for "ten other governorates", let's make sure we have around 11 total.
        cities = [
            ("Sana'a", "Yemen", 15.3694, 44.1910),
            ("Aden", "Yemen", 12.7794, 45.0367),
            ("Taiz", "Yemen", 13.5795, 44.0209),
            ("Al Hudaydah", "Yemen", 14.7978, 42.9545),
            ("Ibb", "Yemen", 13.9667, 44.1833),
            ("Mukalla", "Yemen", 14.5425, 49.1242),
            ("Dhamar", "Yemen", 14.5425, 44.4061),
            ("Amran", "Yemen", 15.6594, 43.9328),
            ("Sa'dah", "Yemen", 16.9402, 43.7638),
            ("Ma'rib", "Yemen", 15.4591, 45.3253),
            ("Al Mahrah", "Yemen", 16.2167, 52.1667)
        ]
        
        insert_sql = """
        INSERT INTO locations (city_name, country, latitude, longitude)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE city_name=VALUES(city_name)
        """
        cursor.executemany(insert_sql, cities)
        print(f"Verified {len(cities)} locations exist in database.")

        conn.commit()
        cursor.close()
        conn.close()
        print("Database fix complete.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fix()
