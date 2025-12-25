import sqlite3
import os

DB_FILE = 'weather.db'

def init_db():
    if os.path.exists(DB_FILE):
        print("Database already exists.")
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    with open('schema.sql', 'r') as f:
        script = f.read()
        cursor.executescript(script)
        
    conn.commit()
    conn.close()
    print("Initialized new SQLite database.")

if __name__ == '__main__':
    init_db()
