import requests
import json
import sqlite3
import time
from datetime import datetime
import os
import re
import xml.etree.ElementTree as ET

# Adjust path to find database in parent directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_FILE = os.path.join(BASE_DIR, 'weather.db')

# Standard browser-like headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9'
}

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def fetch_world_bank_edu(conn):
    """Fetches high-level education indicators from World Bank API."""
    print("--- [World Bank] Fetching Strategic Education Indicators ---")
    
    # Expanded indicators
    indicators = {
        'literacy_rate': 'SE.ADT.LITR.ZS',
        'primary_enrollment': 'SE.PRM.ENRR',
        'secondary_enrollment': 'SE.SEC.ENRR',
        'primary_completion': 'SE.PRM.CMPT.ZS',
        'government_expenditure_edu': 'SE.XPD.TOTL.GD.ZS', # % of GDP
        'out_of_school_primary': 'SE.PRM.UNER',
        'pupil_teacher_ratio': 'SE.PRM.ENRL.TC.ZS'
    }
    
    base_url = "https://api.worldbank.org/v2/country/YEM/indicator/"
    cursor = conn.cursor()
    
    for key, code in indicators.items():
        try:
            # Fetch long history for trend analysis
            url = f"{base_url}{code}?format=json&per_page=30"
            resp = requests.get(url, headers=HEADERS, timeout=15)
            
            if resp.status_code == 200:
                json_data = resp.json()
                if len(json_data) > 1 and json_data[1] is not None:
                    series = sorted(json_data[1], key=lambda x: str(x['date']))
                    
                    # Find latest non-null
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
                        
                    # History for charts
                    history_list = [{'year': item['date'], 'value': item['value']} for item in series if item['value'] is not None]
                    history_json = json.dumps(history_list)
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO education_indicators (indicator_key, current_value, year_updated, history_json, updated_at)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (key, current_value, year_updated, history_json))
                    print(f"  [OK] WB Indicator {key}: {current_value} ({year_updated})")
                else:
                    print(f"  [WARN] No data returned for {key}")
        except Exception as e:
            print(f"  [ERROR] World Bank fetch failed for {key}: {e}")
    conn.commit()

def fetch_reliefweb_rss_education(conn):
    """Fetches broad range of reports (Education + Child Protection) & mines specific dashboard stats."""
    print("--- [ReliefWeb] Fetching Broad Sector Reports (RSS) & Mining Stats ---")
    try:
        # Broaden search: Education OR Protection OR Children to miss nothing
        rss_url = "https://reliefweb.int/updates/rss.xml?search=primary_country.name:%22Yemen%22%20AND%20theme.name:(%22Education%22%20OR%20%22Protection%22%20OR%20%22Children%22)"
        
        resp = requests.get(rss_url, headers=HEADERS, timeout=25)
        if resp.status_code == 200:
            try:
                # Regex mining patterns for education dashboard
                patterns = {
                    'live_out_of_school': r'(?i)(out of school|no access to education).*?(\d{1,3}(?:,\d{3})*).*?children',
                    'live_schools_damaged': r'(?i)(damaged|destroyed|affected).*?(\d{1,3}(?:,\d{3})*).*?schools',
                    'live_teachers_unpaid': r'(?i)(teachers|staff).*?(\d{1,3}(?:,\d{3})*).*?(without salaries|unpaid)',
                    'live_students_affected': r'(?i)(\d{1,3}(?:,\d{3})*).*?students.*?affected',
                    # New: Logic for closure drivers
                    'live_closure_flood': r'(?i)(flood|rain).*?(\d{1,3}(?:,\d{3})*).*?schools',
                    'live_closure_conflict': r'(?i)(conflict|airstrike|shelling).*?(\d{1,3}(?:,\d{3})*).*?schools',
                    # New: Salary/Incentive mentions (binary or count)
                    'live_teacher_incentives': r'(?i)(incentive|stipend).*?(\d{1,3}(?:,\d{3})*).*?teachers'
                }

                root = ET.fromstring(resp.content)
                channel = root.find('channel')
                items = channel.findall('item')
                
                cursor = conn.cursor()
                count = 0
                stats_found = 0
                
                # Keywords to filter broad reports for relevance
                relevance_keywords = ['school', 'education', 'teacher', 'student', 'classroom', 'university', 'curriculum', 'literacy']

                def parse_number(num_str):
                    return int(num_str.replace(',', ''))

                for item in items:
                    title = item.find('title').text if item.find('title') is not None else "Unknown"
                    desc = item.find('description').text if item.find('description') is not None else ""
                    link = item.find('link').text if item.find('link') is not None else "#"
                    pub_date_raw = item.find('pubDate').text if item.find('pubDate') is not None else ""
                    
                    combined_text = (title + " " + desc).lower()
                    
                    # 1. Relevance Filter: Only keep if it mentions education-related terms
                    if not any(k in combined_text for k in relevance_keywords):
                        continue

                    # Parse Date
                    try:
                        dt = datetime.strptime(pub_date_raw[:16], '%a, %d %b %Y')
                        date_published = dt.strftime('%Y-%m-%d')
                    except:
                        date_published = datetime.now().strftime('%Y-%m-%d')
                    
                    # Store Report
                    cursor.execute("""
                        INSERT OR IGNORE INTO situation_reports (sector, title, source, date_published, url)
                        VALUES (?, ?, ?, ?, ?)
                    """, ('education', title, 'ReliefWeb (Broad Scan)', date_published, link))
                    if cursor.rowcount > 0:
                        count += 1
                        
                    # 2. Text Mining for Stats
                    # Use original case-sensitive text for regex? patterns use (?i) so lower is fine or raw.
                    # Patterns are compiled with (?i) so we pass raw text to be safe with casing if needed.
                    raw_text = title + " " + desc
                    
                    for key, pattern in patterns.items():
                        match = re.search(pattern, raw_text)
                        if match:
                            try:
                                # Determine which group has the number
                                # Most patterns are: (prefix)...(number)...
                                # Group 1 is usually the prefix, Group 2 the number.
                                # Exception: 'live_students_affected' where Group 1 is number.
                                
                                if key == 'live_students_affected':
                                     number_str = match.group(1)
                                else:
                                     number_str = match.group(2)
                                     
                                val = parse_number(number_str)
                                
                                if 10 < val < 10000000:
                                    # Accept 2023+ to ensure we capture "last year" as requested
                                    if any(y in date_published for y in ['2023', '2024', '2025']):
                                        snippet = match.group(0)[:100] + "..."
                                        cursor.execute("""
                                            INSERT OR REPLACE INTO education_indicators (indicator_key, current_value, year_updated, history_json, updated_at)
                                            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                                        """, (key, val, date_published, json.dumps([{'source': title, 'snippet': snippet}])))
                                        print(f"  [Insight] Found {key}: {val} in '{title}'")
                                        stats_found += 1
                            except:
                                pass
                                
                conn.commit()
                print(f"  [OK] Synced {count} relevant education reports (Broad Scan). Extracted {stats_found} stats.")

            except Exception as xml_e:
                 print(f"  [ERROR] RSS XML Parsing failed: {xml_e}")
        else:
            print(f"  [WARN] ReliefWeb RSS returned status {resp.status_code}")
    except Exception as e:
        print(f"  [ERROR] ReliefWeb RSS fetch failed: {e}")

def should_run_update(conn):
    """Checks if the data is older than 30 days."""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT MAX(updated_at) FROM education_indicators")
        last_update = cursor.fetchone()[0]
        
        if not last_update:
            return True 
            
        last_run = datetime.strptime(last_update, "%Y-%m-%d %H:%M:%S")
        days_diff = (datetime.now() - last_run).days
        
        if days_diff < 30:
            print(f"--- [SKIP] Data is fresh ({days_diff} days old). Next update due in {30 - days_diff} days. ---")
            return False
            
        print(f"--- [UPDATE] Data is {days_diff} days old. Starting Monthly Refresh... ---")
        return True
    except Exception as e:
        print(f"[WARN] Could not check freshness: {e}. Defaulting to RUN.")
        return True

def cleanup_stale_data(conn):
    """Deletes data that wasn't updated in the current run (older than 24h)."""
    print("--- [CLEANUP] Removing stale data (>1 day old) ---")
    cursor = conn.cursor()
    
    # Remove indicators not touched in this run
    cursor.execute("DELETE FROM education_indicators WHERE updated_at < datetime('now', '-1 day')")
    removed_ind = cursor.rowcount
    
    # Remove old reports (keeping last 1 year)
    cursor.execute("DELETE FROM situation_reports WHERE sector='education' AND date_published < datetime('now', '-1 year')")
    removed_rep = cursor.rowcount
    
    conn.commit()
    print(f"  [OK] Cleaned {removed_ind} stale indicators and {removed_rep} old reports.")

def run_etl():
    print(f"=== Yemen Education Intelligence ETL Engine v2.5 (Monthly) ===")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    conn = get_db_connection()
    
    if should_run_update(conn):
        # 1. Strategic Indicators
        fetch_world_bank_edu(conn)
        
        # 2. Operational Reports & Live Text Mining
        fetch_reliefweb_rss_education(conn)
        
        # 3. Fallback/Baseline Projections
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO education_indicators (indicator_key, current_value, year_updated, history_json, updated_at)
            VALUES 
                ('projected_out_of_school', 4500000, '2025', '[]', CURRENT_TIMESTAMP),
                ('projected_schools_damaged', 2500, '2025', '[]', CURRENT_TIMESTAMP),
                ('projected_teachers_unpaid', 190000, '2025', '[]', CURRENT_TIMESTAMP)
        """)
        conn.commit()
        
        # 4. Cleanup
        cleanup_stale_data(conn)
        
        print(f"\n--- Education ETL Completed Successfully ---")
    else:
        print(f"\n--- Education ETL Skipped (Data is Up-to-Date) ---")
        
    conn.close()

if __name__ == "__main__":
    run_etl()
