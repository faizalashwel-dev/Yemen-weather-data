import mysql.connector
from db_config import DB_CONFIG

def migrate():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        columns = [
            ("humidity", "FLOAT"),
            ("pressure", "FLOAT"),
            ("uv_index", "FLOAT"),
            ("dew_point", "FLOAT"),
            ("visibility", "FLOAT"),
            ("cloud_cover", "FLOAT"),
            ("solar_rad", "FLOAT")
        ]
        
        tables = ["current_weather", "weather_history"]
        
        for table in tables:
            print(f"Migrating table: {table}")
            cursor.execute(f"DESCRIBE {table}")
            existing_columns = [row[0] for row in cursor.fetchall()]
            
            for col_name, col_type in columns:
                if col_name not in existing_columns:
                    print(f"Adding column {col_name} to {table}...")
                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
                else:
                    print(f"Column {col_name} already exists in {table}")
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Migration completed successfully.")
        
    except Exception as e:
        print(f"Migration Error: {e}")

if __name__ == "__main__":
    migrate()
