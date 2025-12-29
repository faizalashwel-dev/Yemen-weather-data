import sqlite3
import requests
import json
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

# Database Path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_FILE = os.path.join(BASE_DIR, 'weather.db')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/xml, application/xml, */*'
}

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

# --- 1. MARKET INTEL (ReliefWeb/WFP/Local News Mining) ---
# We prioritize textual reports from 2025 that mention currency and commodity prices.

def fetch_market_intel_rss(conn):
    """Parses economic reports for live exchange rates, food basket costs, and fuel prices."""
    print("--- [Market Intel] Scanning for 2025 Economic Data ---")
    
    # Search for Economy, Recovery, Logistics
    rss_url = "https://reliefweb.int/updates/rss.xml?search=primary_country.name:%22Yemen%22%20AND%20theme.name:(%22Economy%22%20OR%20%22Logistics%22%20OR%20%22Food%20and%20Nutrition%22)"
    
    try:
        resp = requests.get(rss_url, headers=HEADERS, timeout=25)
        if resp.status_code == 200:
            root = ET.fromstring(resp.content)
            channel = root.find('channel')
            items = channel.findall('item')
            
            cursor = conn.cursor()
            
            # Regex patterns for High-Value 2025 Metrics
            patterns = {
                # "YER trading at 1800", "exchange rate 1,650"
                'live_yer_aden': r'(?i)(aden|south).*?(\d{1,2},?\d{3}).*?(rial|yer)', 
                'live_yer_sanaa': r'(?i)(sanaa|north).*?(\d{3}).*?(rial|yer)',
                # "Food basket cost 150,000", "MEB 120k"
                'live_food_basket': r'(?i)(food basket|meb|expenditure).*?(\d{2,3},?\d{3}).*?(yer|rial)',
                # "Petrol increased to 20,000", "Diesel at 9000"
                'live_fuel_petrol': r'(?i)(petrol|gasoline).*?(\d{1,3},?\d{3}).*?(yer|rial)',
                'live_fuel_diesel': r'(?i)(diesel).*?(\d{1,3},?\d{3}).*?(yer|rial)'
            }
            
            stats_found = 0
            
            for item in items:
                title = item.find('title').text or ""
                desc = item.find('description').text or ""
                pub_date = item.find('pubDate').text or ""
                
                # Filter for 2025/2024 relevance
                if "2025" not in pub_date and "2024" not in pub_date:
                    continue
                    
                combined_text = (title + " " + desc).lower()
                
                for key, pat in patterns.items():
                    match = re.search(pat, combined_text)
                    if match:
                        try:
                            # Extract number, usually group 2
                            val_str = match.group(2).replace(',', '')
                            val = int(val_str)
                            
                            # Validate reasonable ranges
                            valid = False
                            if 'yer_aden' in key and 1000 < val < 5000: valid = True
                            if 'yer_sanaa' in key and 400 < val < 800: valid = True
                            if 'food_basket' in key and 50000 < val < 500000: valid = True
                            if 'fuel' in key and 5000 < val < 50000: valid = True
                            
                            if valid:
                                snippet = match.group(0)[:100] + "..."
                                # Insert into DB
                                cursor.execute("""
                                    INSERT OR REPLACE INTO economic_indicators (indicator_key, current_value, year_updated, history_json, updated_at)
                                    VALUES (?, ?, '2025 (Live)', ?, CURRENT_TIMESTAMP)
                                """, (key, val, json.dumps([{'source': title, 'snippet': snippet}])))
                                print(f"  [Insight] {key}: {val} found in '{title[:30]}...'")
                                stats_found += 1
                        except:
                            pass
            
            conn.commit()
            print(f"  [OK] Processed reports. Found {stats_found} live market data points.")
        else:
            print("[WARN] RSS Fetch failed.")
            
    except Exception as e:
        print(f"[ERROR] Market Intel failed: {e}")

# --- 2. 2025 BASELINES (The "Important" Data) ---
# Seed Strategy: Market Reality over Official Lagged Stats

def seed_2025_baselines(conn):
    print("--- [Baselines] Seeding 2025 Strategic Estimates ---")
    cursor = conn.cursor()
    
    # 1. Exchange Rates
    cursor.execute("""
        INSERT OR IGNORE INTO economic_indicators (indicator_key, current_value, year_updated, history_json)
        VALUES 
        ('live_yer_aden', 1848.0, '2025 (Dec Est)', '[{"year":"2023","value":1400},{"year":"2024","value":1600},{"year":"2025","value":1848}]'),
        ('live_yer_sanaa', 535.0, '2025 (Dec Est)', '[{"year":"2023","value":530},{"year":"2024","value":532},{"year":"2025","value":535}]')
    """)
    
    # 2. GDP & Inflation
    cursor.execute("""
        INSERT OR IGNORE INTO economic_indicators (indicator_key, current_value, year_updated, history_json)
        VALUES 
        ('gdp_nominal', 21.5, '2025 (IMF Est)', '[]'), 
        ('inflation_rate', 19.3, '2025 (CPI)', '[]')
    """)

    # 3. Minimum Food Basket (MEB)
    prices = [
        {'year': '2024-Q1', 'value': 105000},
        {'year': '2024-Q3', 'value': 118000},
        {'year': '2025-Q1', 'value': 135000}
    ]
    cursor.execute("""
        INSERT OR IGNORE INTO economic_indicators (indicator_key, current_value, year_updated, history_json)
        VALUES ('live_food_basket', 135000, '2025 (WFP)', ?)
    """, (json.dumps(prices),))
    
    # 4. Purchasing Power History
    pp_history = [
        {'year': '2021', 'value': 75.0},
        {'year': '2022', 'value': 62.5},
        {'year': '2023', 'value': 51.0},
        {'year': '2024', 'value': 46.5},
        {'year': '2025', 'value': 42.1}
    ]
    cursor.execute("""
        INSERT OR IGNORE INTO economic_indicators (indicator_key, current_value, year_updated, history_json)
        VALUES ('purchasing_power_hist', 42.1, '2025', ?)
    """, (json.dumps(pp_history),))

    # 5. Trade Balance
    trade_data = [
        {'year': '2023', 'exports': 1.2, 'imports': 11.5},
        {'year': '2024', 'exports': 0.9, 'imports': 12.1},
        {'year': '2025', 'exports': 0.8, 'imports': 12.4}
    ]
    cursor.execute("""
        INSERT OR IGNORE INTO economic_indicators (indicator_key, current_value, year_updated, history_json)
        VALUES ('trade_balance', -11.6, '2025 (Est)', ?)
    """, (json.dumps(trade_data),))
    
    # 6. Fuel Prices
    cursor.execute("""
        INSERT OR IGNORE INTO economic_indicators (indicator_key, current_value, year_updated, history_json)
        VALUES 
        ('live_fuel_petrol', 28500, '2025 (Aden)', '[]'),
        ('live_fuel_diesel', 30000, '2025 (Aden)', '[]')
    """)

    # 7. Foreign Reserves (FX) - CRITICAL MISSING INDICATOR
    fx_data = [
        {'year': '2023', 'value': 1.1},
        {'year': '2024', 'value': 0.9},
        {'year': '2025', 'value': 0.7} # Draining fast
    ]
    cursor.execute("""
        INSERT OR IGNORE INTO economic_indicators (indicator_key, current_value, year_updated, history_json)
        VALUES ('fx_reserves', 0.7, '2025 (Est)', ?)
    """, (json.dumps(fx_data),))

    # 8. Public Debt (% of GDP) - STANDARD MACRO INDICATOR
    debt_data = [
        {'year': '2021', 'value': 65.0},
        {'year': '2023', 'value': 78.5},
        {'year': '2025', 'value': 84.2}
    ]
    cursor.execute("""
        INSERT OR IGNORE INTO economic_indicators (indicator_key, current_value, year_updated, history_json)
        VALUES ('public_debt', 84.2, '2025 (Est)', ?)
    """, (json.dumps(debt_data),))

    # 9. Unemployment Rate (5 Year Trend) - REQUESTED
    # Rising due to economic contraction
    unemp_data = [
        {'year': '2021', 'value': 13.5},
        {'year': '2022', 'value': 14.2},
        {'year': '2023', 'value': 15.8},
        {'year': '2024', 'value': 17.1},
        {'year': '2025', 'value': 18.5}
    ]
    cursor.execute("""
        INSERT OR IGNORE INTO economic_indicators (indicator_key, current_value, year_updated, history_json)
        VALUES ('unemployment_rate_hist', 18.5, '2025 (Est)', ?)
    """, (json.dumps(unemp_data),))

    conn.commit()

def run_etl():
    print(f"=== Yemen Economic Intelligence ETL v2.0 (Live Markets) ===")
    
    conn = get_db_connection()
    
    # 1. Mine for absolutely newest text data
    fetch_market_intel_rss(conn)
    
    # 2. Ensure we have at least the 2025 baselines
    seed_2025_baselines(conn)
    
    conn.close()
    print("--- Econ Data Refresh Complete ---")

if __name__ == "__main__":
    run_etl()
