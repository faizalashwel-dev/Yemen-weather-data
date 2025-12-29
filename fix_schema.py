import sqlite3

DB_FILE = 'weather.db'

def fix_tables():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Check health_indicators
    cursor.execute("PRAGMA table_info(health_indicators)")
    cols = [row[1] for row in cursor.fetchall()]
    if 'updated_at' not in cols:
        print("Adding updated_at to health_indicators...")
        cursor.execute("ALTER TABLE health_indicators ADD COLUMN updated_at TIMESTAMP")
    
    # Re-run the situation_reports table creation just in case
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS situation_reports (
        report_id INTEGER PRIMARY KEY AUTOINCREMENT,
        sector TEXT NOT NULL,
        title TEXT NOT NULL,
        source TEXT,
        date_published TEXT,
        url TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(url)
    )
    """)
    
    conn.commit()
    conn.close()
    print("Tables fixed.")

if __name__ == "__main__":
    fix_tables()
