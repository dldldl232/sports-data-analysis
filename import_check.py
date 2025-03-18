import requests
import json  # Import JSON for formatting

# ESPN API endpoint for NBA Scoreboard
url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"

# Fetch the data from ESPN
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    data_espn = response.json()  # Convert response to JSON

    # Print entire JSON response in a formatted way
    print(json.dumps(data_espn, indent=2))  # Pretty print the JSON response
else:
    print(f"Failed to fetch data: {response.status_code}")
