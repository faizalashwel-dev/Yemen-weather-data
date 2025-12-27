-- SQLite Schema
CREATE TABLE IF NOT EXISTS locations (
    location_id INTEGER PRIMARY KEY AUTOINCREMENT,
    city_name TEXT NOT NULL,
    country TEXT,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(latitude, longitude)
);
CREATE TABLE IF NOT EXISTS current_weather (
    weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id INTEGER NOT NULL,
    observation_time TEXT NOT NULL,
    temperature REAL,
    humidity REAL,
    windspeed REAL,
    winddirection INTEGER,
    weathercode INTEGER,
    is_day BOOLEAN,
    pressure REAL,
    uv_index REAL,
    dew_point REAL,
    visibility INTEGER,
    cloud_cover INTEGER,
    solar_rad REAL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (location_id) REFERENCES locations(location_id) ON DELETE CASCADE,
    UNIQUE(location_id)
);
CREATE TABLE IF NOT EXISTS weather_history (
    history_id INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id INTEGER NOT NULL,
    observation_time TEXT NOT NULL,
    temperature REAL,
    humidity REAL,
    windspeed REAL,
    winddirection INTEGER,
    weathercode INTEGER,
    is_day BOOLEAN,
    pressure REAL,
    uv_index REAL,
    dew_point REAL,
    visibility INTEGER,
    cloud_cover INTEGER,
    solar_rad REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (location_id) REFERENCES locations(location_id) ON DELETE CASCADE,
    UNIQUE(location_id, observation_time)
);
CREATE INDEX IF NOT EXISTS idx_history_time ON weather_history (observation_time);
CREATE INDEX IF NOT EXISTS idx_history_location_time ON weather_history (location_id, observation_time);
-- Initial Locations
INSERT
    OR IGNORE INTO locations (city_name, country, latitude, longitude)
VALUES ('Sana''a', 'Yemen', 15.3694, 44.1910),
    ('Aden', 'Yemen', 12.7794, 45.0367),
    ('Taiz', 'Yemen', 13.5795, 44.0209),
    ('Al Hudaydah', 'Yemen', 14.7978, 42.9545),
    ('Ibb', 'Yemen', 13.9667, 44.1833),
    ('Mukalla', 'Yemen', 14.5425, 49.1242),
    ('Dhamar', 'Yemen', 14.5425, 44.4061),
    ('Amran', 'Yemen', 15.6594, 43.9328),
    ('Sa''dah', 'Yemen', 16.9402, 43.7638),
    ('Ma''rib', 'Yemen', 15.4591, 45.3253),
    ('Al Mahrah', 'Yemen', 16.2167, 52.1667);
-- Health Indicators Table
CREATE TABLE IF NOT EXISTS health_indicators (
    indicator_id INTEGER PRIMARY KEY AUTOINCREMENT,
    indicator_key TEXT NOT NULL UNIQUE,
    current_value REAL,
    year_updated TEXT,
    history_json TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Seed Health Indicators
INSERT
    OR IGNORE INTO health_indicators (
        indicator_key,
        current_value,
        year_updated,
        history_json
    )
VALUES (
        'life_expectancy',
        67.2,
        '2023',
        '[{"year": "2021", "value": 66.8}, {"year": "2022", "value": 67.0}, {"year": "2023", "value": 67.2}]'
    ),
    (
        'mortality_rate',
        58.4,
        '2023',
        '[{"year": "2021", "value": 60.1}, {"year": "2022", "value": 59.2}, {"year": "2023", "value": 58.4}]'
    ),
    (
        'population',
        40000000,
        '2024',
        '[{"year": "2023", "value": 39000000}, {"year": "2024", "value": 40000000}]'
    );