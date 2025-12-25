import mysql.connector
from db_config import DB_CONFIG

def setup_database():
    try:
        # Connect to MySQL server (no database selected yet)
        # Create a copy of config without 'database' key to connect to server first
        server_config = DB_CONFIG.copy()
        if 'database' in server_config:
            del server_config['database']
            
        print("Connecting to MySQL server...")
        conn = mysql.connector.connect(**server_config)
        cursor = conn.cursor()

        # Read schema.sql
        print("Reading schema.sql...")
        with open('schema.sql', 'r') as f:
            sql_script = f.read()

        # Execute statements
        print("Executing SQL statements...")
        # Simple split by semicolon. 
        # Note: This is a basic parser and works for this specific schema. 
        # For complex schemas with triggers/procedures, a more robust parser is needed.
        commands = sql_script.split(';')
        for command in commands:
            cleaned_command = command.strip()
            if cleaned_command:
                try:
                    cursor.execute(cleaned_command)
                except mysql.connector.Error as err:
                    print(f"Statement skipped or failed: {err}")

            
        print("Database structure created successfully.")
        
        cursor.close()
        conn.close()
        
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    setup_database()
