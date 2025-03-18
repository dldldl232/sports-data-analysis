import requests
import json
import pandas as pd
from datetime import datetime

# ESPN API endpoint for NBA scoreboard
url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
response = requests.get(url)

data = {}

if response.status_code == 200:
    data = response.json()  # converting data to json format
    print("Data Fetched Successfully")

    print(json.dumps(data, indent=4))
else:
    print(f"Failed to fetch data for {response.status_code}")

# Extracting game details
games = data.get("events", [])
