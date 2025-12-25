#!/bin/bash
# Initialize DB if not exists
python init_db.py

# Run the fetcher in the background
python weather_fetcher.py &

# Run the web server
gunicorn app:app
