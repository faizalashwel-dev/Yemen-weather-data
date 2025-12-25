# Database Configuration
# REPLACE 'your_password' with your actual MySQL root password
import os
import sqlite3

DB_FILE = 'weather.db'

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row  # Access columns by name
    return conn
