CREATE DATABASE IF NOT EXISTS weather_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE weather_db;
CREATE TABLE IF NOT EXISTS locations (
    location_id INT AUTO_INCREMENT PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    country VARCHAR(100),
    latitude DECIMAL(8, 5) NOT NULL,
    longitude DECIMAL(8, 5) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_location (latitude, longitude)
);
CREATE TABLE IF NOT EXISTS current_weather (
    weather_id INT AUTO_INCREMENT PRIMARY KEY,
    location_id INT NOT NULL,
    observation_time DATETIME NOT NULL,
    temperature FLOAT,
    humidity FLOAT,
    windspeed FLOAT,
    winddirection INT,
    weathercode INT,
    is_day BOOLEAN,
    pressure FLOAT,
    uv_index FLOAT,
    dew_point FLOAT,
    visibility INT,
    cloud_cover INT,
    solar_rad FLOAT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_current_location FOREIGN KEY (location_id) REFERENCES locations(location_id) ON DELETE CASCADE,
    UNIQUE KEY uk_current_weather (location_id, observation_time)
);
CREATE TABLE IF NOT EXISTS weather_history (
    history_id INT AUTO_INCREMENT PRIMARY KEY,
    location_id INT NOT NULL,
    observation_time DATETIME NOT NULL,
    temperature FLOAT,
    humidity FLOAT,
    windspeed FLOAT,
    winddirection INT,
    weathercode INT,
    is_day BOOLEAN,
    pressure FLOAT,
    uv_index FLOAT,
    dew_point FLOAT,
    visibility INT,
    cloud_cover INT,
    solar_rad FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_history_location FOREIGN KEY (location_id) REFERENCES locations(location_id) ON DELETE CASCADE,
    UNIQUE KEY uk_weather_history (location_id, observation_time)
);
CREATE INDEX idx_history_time ON weather_history (observation_time);
CREATE INDEX idx_history_location_time ON weather_history (location_id, observation_time);
INSERT INTO locations (city_name, country, latitude, longitude)
VALUES ('Sana''a', 'Yemen', 15.3694, 44.1910) ON DUPLICATE KEY
UPDATE city_name = city_name;