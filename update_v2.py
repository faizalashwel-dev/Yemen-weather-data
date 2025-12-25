import mysql.connector
from db_config import DB_CONFIG

def update_database():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 1. Alter Schema: Add 'country' column if not exists
        # There's no "IF NOT EXISTS" for ADD COLUMN in simplified MySQL, 
        # so we'll wrap in try-catch or check `information_schema`.
        # For simplicity, we just try to add it.
        
        print("Adding 'country' column to tables...")
        tables = ['current_weather', 'weather_history']
        for table in tables:
            try:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN country VARCHAR(100) AFTER location_id")
                print(f"Added 'country' to '{table}'.")
            except mysql.connector.Error as err:
                if err.errno == 1060: # Duplicate column name
                    print(f"'country' column already exists in '{table}'.")
                else:
                    print(f"Error altering '{table}': {err}")

        # 2. Add Yemeni Cities
        print("Inserting cities...")
        cities = [
            ("Aden", "Yemen", 12.7794, 45.0367),
            ("Taiz", "Yemen", 13.5795, 44.0209),
            ("Al Hudaydah", "Yemen", 14.7978, 42.9545),
            ("Ibb", "Yemen", 13.9667, 44.1833),
            ("Mukalla", "Yemen", 14.5425, 49.1242),
            ("Dhamar", "Yemen", 14.5425, 44.4061),
            ("Amran", "Yemen", 15.6594, 43.9328),
            ("Sa'dah", "Yemen", 16.9402, 43.7638)
        ]
        
        insert_sql = """
        INSERT INTO locations (city_name, country, latitude, longitude)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE city_name=VALUES(city_name), country=VALUES(country)
        """
        
        cursor.executemany(insert_sql, cities)
        print(f"Inserted/Updated {cursor.rowcount} cities.")

        conn.commit()
        cursor.close()
        conn.close()
        print("Database update complete.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

if __name__ == "__main__":
    update_database()
