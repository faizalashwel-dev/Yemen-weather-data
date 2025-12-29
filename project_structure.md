# Yemen Crisis Analytics & Intelligence System - Technical Deep-Dive

This document provides a comprehensive technical breakdown of the Yemen Analytics Command Center, detailing every architectural layer, data point origin, and background automation process.

## 1. Tactical Workflow & Data Pipeline

The system is engineered as a three-tier high-availability platform to monitor Yemen's atmospheric and humanitarian situation.

### A. Data Ingestion Layer (The Sensors)
The project uses autonomous background processes to maintain a "Digital Twin" of Yemen's state.
-   **Weather Bot (`weather_fetcher.py`)**: 
    -   **Loop**: Runs every 300 seconds (5 minutes).
    -   **Source**: Open-Meteo V1 Forecast API.
    -   **Intelligence**: Fetches 14 distinct variables (Temp, Humidity, Apparent Temp, UV, Wind Speed/Dir, Pressure, Visibility, Cloud Cover, Solar Radiation).
    -   **Storage**: Performs an `INSERT OR REPLACE` (UPSERT) into the `current_weather` table to keep the "Live" state fresh, and an `INSERT OR IGNORE` into `weather_history` for temporal analysis.
-   **Strategic ETLs (`etl/`)**: 
    -   Modularized scripts for long-term indicator tracking.
    -   **Health (`etl/etl_health.py`)**: Pulls World Bank and ReliefWeb health reports.
    -   **Education (`etl/etl_education.py`)**: Pulls educational metrics and systemic risk projections.
    -   **Runner**: `run_all_etls.py` executes the entire strategic update pipeline.

### B. Core Intelligence Layer (The Backend)
The Flask-based API (`app.py`) serves as the central hub for mapping data into specific operational contexts.
-   **Weather API (`/api/weather`)**: Direct database-to-browser pipe for atmospheric telemetry.
-   **Health API (`/api/health`)**: Aggregates three data streams:
    1.  **Local SQLite Cache**: High-level indicators (Life expectancy, etc.).
    2.  **Live Population.io**: Real-time demographic counter.
    3.  **ReliefWeb (OCHA)**: Real-time situational reports (Filtering: `primary_country: "Yemen"` AND `theme: "Health"`).
-   **Education API (`/api/education`)**: A predictive simulation engine that models school status based on strategic baselines (literacy, unpaid salary data) to show what regional crises look like on the ground.

---

## 2. Database Schema (SQLite)

The database `weather.db` is the single source of truth for all logged events.

-   **`locations`**: Metadata for the 11 audited Governorates (Sana'a, Aden, Taiz, Hudaydah, Ibb, Mukalla, Dhamar, Amran, Sa'dah, Marib, Al Mahrah) including precise Lat/Lon coordinates.
-   **`current_weather`**: A performance-optimized table with a `UNIQUE(location_id)` constraint, ensuring O(1) lookups for the "Current State" of any city.
-   **`weather_history`**: A temporal log containing thousands of records to fuel the "Temporal Energy Gradient" (Line Charts).
-   **`health_indicators`**: Stores World Bank datasets in a `history_json` blob format, enabling the frontend to render historical line charts without complex joins.

---

## 3. Visualization Logic

The frontend translates raw data into tactical intelligence using several advanced visualization techniques:

### I. Geospatial Intelligence (Leaflet.js)
-   **Boundary Rendering**: Pulls high-precision ADM1 boundaries of Yemen.
-   **Thermal Shading**: Governorates change color dynamically based on their real-time temperature (e.g., Al Hudaydah turning deep red in high heat).
-   **Neural Wind Flow**: A custom HTML5 Canvas engine overlays the map, spawning particles that move according to the **real wind speed and direction** recorded in the database, constrained specifically within governorate boundaries.

### II. Analytical Profiling (Chart.js)
-   **Atmospheric Radar**: A multi-dimensional check of Heat, Wind, Humidity, Sky Density, and UV Intensity. The radar "spikes" when conditions become dangerous.
-   **Temporal Gradient**: A time-series chart that tracks the "Energy Gradient" (Temperature change) over the last 3 hours using actual history logs.
-   **Disease Pressure Grid**: Uses a custom CSS Grid to rank regions by hospital pressure using a combination of WHO reports and simulated caseload variation.

---

## 4. Truth Matrix: Data Authenticity Audit

| Category | Indicator | Authenticity | Method/Verification |
| :--- | :--- | :--- | :--- |
| **Telemetry** | Temperature / Wind | **REAL** | Verified via `verify_weather.py` (Meteo Sync) |
| **Demographics**| Population | **REAL** | Live fetch from Population.io / UN DESA |
| **Intelligence** | NGO Field Reports | **REAL** | RSS/API feed from ReliefWeb (OCHA) |
| **Epidemiology**| Cholera / Mortality| **STATIC-REAL**| Hardcoded from WHO/OCHA 2024 End-of-Year reports |
| **Education** | Out-of-school/Damage| **STATIC-REAL**| Verified 2025 Strategic Projections |
| **Simulation** | School Attendance | **SIMULATED** | Deterministic math modelling of regional crisis |
| **Simulation** | Ambulance Activity | **SIMULATED** | Visual demo of response capability |

---

## 5. Verification Tools

-   **`verify_weather.py`**: Compares the database state against a fresh API call for every city and calculates the variance (Pass/Fail).
-   **`verify_system.py`**: A comprehensive health check for the entire architecture.

---
**Technical Maintainer**: Antigravity AI
**Version**: 2.5 (High Detail)
