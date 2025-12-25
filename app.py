from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from db_config import get_db_connection
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

@app.route('/')
def index():
    return send_from_directory('.', 'dashboard.html')

@app.route('/api/weather')
def get_weather():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # FETCH FROM DATABASE
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
        cities = [dict(row) for row in cursor.fetchall()]

        # History Fetch (Last 3 hours)
        limit = (datetime.now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
        query_history = """
            SELECT l.city_name, wh.temperature, wh.observation_time 
            FROM weather_history wh 
            JOIN locations l ON wh.location_id = l.location_id 
            WHERE wh.observation_time > ? 
            ORDER BY wh.observation_time ASC
        """
        cursor.execute(query_history, (limit,))
        history = [dict(row) for row in cursor.fetchall()]
        
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
