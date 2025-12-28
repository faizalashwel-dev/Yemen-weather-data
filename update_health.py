import requests
import json
import sqlite3
import time

DB_FILE = 'weather.db'

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def update_health_data():
    print("Fetching Health Data from World Bank API...")
    
    indicators = {
        'life_expectancy': 'SP.DYN.LE00.IN',
        'mortality_rate': 'SH.DYN.MORT',
        'health_expenditure': 'SH.XPD.CHEX.GD.ZS',
        'measles_immunization': 'SH.IMM.MEAS',
        'population': 'SP.POP.TOTL',
        'birth_rate': 'SP.DYN.CBRT.IN',
        'death_rate': 'SP.DYN.CDRT.IN'
    }
    
    base_url = "https://api.worldbank.org/v2/country/YEM/indicator/"
    
    conn = get_db_connection()
    c = conn.cursor()
    
    for key, code in indicators.items():
        try:
            print(f"Fetching {key}...")
            # Fetch history (last 20 years)
            resp = requests.get(f"{base_url}{code}?format=json&per_page=20")
            
            if resp.status_code == 200:
                json_data = resp.json()
                if len(json_data) > 1:
                    series = sorted(json_data[1], key=lambda x: x['date'])
                    
                    # Get most recent non-null
                    latest_val = None
                    for item in reversed(series):
                        if item['value'] is not None:
                            latest_val = item
                            break
                    
                    if latest_val:
                        current_value = latest_val['value']
                        year_updated = latest_val['date']
                    else:
                        current_value = 0
                        year_updated = "N/A"
                        
                    # Prepare history JSON
                    history_list = [{'year': item['date'], 'value': item['value']} for item in series if item['value'] is not None]
                    history_json = json.dumps(history_list)
                    
                    # Insert/Update DB
                    c.execute("""
                        INSERT OR REPLACE INTO health_indicators (indicator_key, current_value, year_updated, history_json)
                        VALUES (?, ?, ?, ?)
                    """, (key, current_value, year_updated, history_json))
                    
                    print(f"Updated {key}: {current_value} ({year_updated})")
                else:
                    print(f"No data for {key}")
            else:
                print(f"Error fetching {key}: {resp.status_code}")
                
        except Exception as e:
            print(f"Exception for {key}: {e}")
            
    conn.commit()
    conn.close()
    print("Health Data Update Complete!")

if __name__ == "__main__":
    update_health_data()
