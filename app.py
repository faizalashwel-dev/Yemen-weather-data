from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import mysql.connector
from db_config import DB_CONFIG
from datetime import datetime, timedelta
import json
import decimal
import os

app = Flask(__name__)
CORS(app)

class EnhancedEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super().default(obj)

def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

@app.route('/')
def index():
    # Serve the dashboard directly
    return send_from_directory('.', 'dashboard.html')

@app.route('/api/weather')
def get_weather():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # FETCH FROM DATABASE ONLY (No external API calls here)
        # We fetch all the scientific columns we just added
        query_current = """
            SELECT l.location_id, l.city_name, l.country, l.latitude, l.longitude,
                   cw.temperature, cw.humidity, cw.windspeed, cw.winddirection, cw.pressure, 
                   cw.uv_index, cw.dew_point, cw.visibility, cw.cloud_cover, 
                   cw.solar_rad, cw.observation_time
            FROM locations l
            LEFT JOIN current_weather cw ON l.location_id = cw.location_id
            ORDER BY l.city_name ASC
        """
        cursor.execute(query_current)
        cities = cursor.fetchall()

        # History Fetch (Last 3 hours)
        limit = (datetime.now() - timedelta(hours=3))
        query_history = """
            SELECT l.city_name, wh.temperature, wh.observation_time 
            FROM weather_history wh 
            JOIN locations l ON wh.location_id = l.location_id 
            WHERE wh.observation_time > %s 
            ORDER BY wh.observation_time ASC
        """
        cursor.execute(query_history, (limit,))
        history = cursor.fetchall()
        
        cursor.close()
        conn.close()

        response_data = {
            'status': 'success',
            'current': cities,
            'history': history,
            'server_time': datetime.now().strftime('%H:%M:%S')
        }
        return json.dumps(response_data, cls=EnhancedEncoder), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    print("Dashboard Backend starting (Database-Only Mode)")
    app.run(host='0.0.0.0', port=5000, debug=True)
