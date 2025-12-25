# Database Configuration
# REPLACE 'your_password' with your actual MySQL root password
import os

# Database Configuration
# Uses environment variables with fallback to local defaults
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', '775032054'),
    'database': os.getenv('DB_NAME', 'weather_db'),
    'port': int(os.getenv('DB_PORT', 3306))
}
