import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_FILE = os.path.join(BASE_DIR, 'weather.db')

conn = sqlite3.connect(DB_FILE)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# Check reports count
cursor.execute("SELECT count(*) FROM situation_reports WHERE sector='education'")
count = cursor.fetchone()[0]
print(f"Total Education Reports: {count}")

# Check latest ones
cursor.execute("SELECT title, date_published FROM situation_reports WHERE sector='education' ORDER BY date_published DESC LIMIT 5")
rows = cursor.fetchall()
print("Latest Reports:")
for row in rows:
    print(f"- {row['date_published']}: {row['title']}")

conn.close()
