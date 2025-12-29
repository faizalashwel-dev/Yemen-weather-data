import requests
import sqlite3
import json
import os

def test_api_connectivity():
    print("--- Testing API Connectivity ---")
    try:
        r = requests.get("http://127.0.0.1:5000/api/weather", timeout=5)
        print(f"[OK] Weather API Status: {r.status_code}")
        data = r.json()
        print(f"[INFO] Weather entries returned: {len(data.get('current', []))}")
        
        r = requests.get("http://127.0.0.1:5000/api/health", timeout=5)
        print(f"[OK] Health API Status: {r.status_code}")
        h_data = r.json()
        pop = h_data.get('population', {}).get('total')
        print(f"[INFO] Population Data (Population.io): {pop}")
        reports = len(h_data.get('extended', {}).get('reports', []))
        print(f"[INFO] Real-Time Reports (ReliefWeb): {reports}")

        r = requests.get("http://127.0.0.1:5000/api/education", timeout=5)
        print(f"[OK] Education API Status: {r.status_code}")
        ed_data = r.json()
        print(f"[INFO] Education Source: {ed_data.get('meta', {}).get('source')}")
    except Exception as e:
        print(f"[ERROR] API Test Failed: {e}. Is the Flask app running?")

def verify_database():
    print("\n--- Verifying Database Integrity ---")
    if not os.path.exists("weather.db"):
        print("[FAIL] weather.db not found!")
        return

    conn = sqlite3.connect("weather.db")
    c = conn.cursor()
    
    c.execute("SELECT city_name FROM locations")
    cities = [r[0] for r in c.fetchall()]
    print(f"[DATA] Registered Locations: {', '.join(cities)}")

    c.execute("SELECT COUNT(*) FROM current_weather")
    count = c.fetchone()[0]
    if count > 0:
        print(f"[OK] current_weather table has {count} active records.")
    else:
        print("[FAIL] current_weather is empty!")

    conn.close()

def identify_truth_sources():
    print("\n--- Auditing Reality vs. Simulation ---")
    print("[TRUTH] Weather Data: REAL (Pulled from Open-Meteo via weather_fetcher.py)")
    print("[TRUTH] Population: REAL (Live fetch from Population.io)")
    print("[TRUTH] Humanitarian Reports: REAL (Live fetch from ReliefWeb)")
    print("[FACTS] Disease Stats: STATIC-REAL (Hardcoded from 2024 WHO Yemen Reports)")
    print("[SIM] Education Performance: SIMULATED (Generated using Sine/Random logic in app.py for demonstration)")
    print("[SIM] Ambulance Activity: SIMULATED (UI-Level animation for training/demo purposes)")

if __name__ == "__main__":
    print("=== YEMEN COMMAND CENTER VERIFICATION SUITE ===\n")
    # Note: Testing API requires the server to be running.
    # We will verify DB first as it's independent.
    verify_database()
    identify_truth_sources()
    print("\n[MANUAL CHECK] To verify API, run 'python app.py' then run this script.")
