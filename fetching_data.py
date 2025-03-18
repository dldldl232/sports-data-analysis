import requests
import pandas as pd
from datetime import datetime
import os


def collect_espn_data():
    # ESPN API endpoint for NBA Scoreboard
    url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"

    # Fetch the data from ESPN
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch data: {response.status_code}")
        return None

    data = response.json()
    events = data.get("events", [])

    collected = []
    # Record the collection timestamp
    collection_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Loop over each event and extract details
    for event in events:
        event_id = event.get("id", "")
        event_name = event.get("name", "")
        event_date = event.get("date", "")

        # Initialize default values for teams and scores
        team1 = team2 = score1 = score2 = ""

        competitions = event.get("competitions", [])
        if competitions:
            competition = competitions[0]
            competitors = competition.get("competitors", [])
            if len(competitors) >= 2:
                team1 = competitors[0]["team"].get("displayName", "")
                team2 = competitors[1]["team"].get("displayName", "")
                score1 = competitors[0].get("score", "")
                score2 = competitors[1].get("score", "")

        # Append extracted data along with the collection timestamp
        collected.append({
            "collection_timestamp": collection_time,
            "event_id": event_id,
            "event_name": event_name,
            "event_date": event_date,
            "team1": team1,
            "score1": score1,
            "team2": team2,
            "score2": score2
        })

    return collected


def save_data_to_csv(data, filename="collected_espn_data.csv"):
    # Create a new DataFrame from collected data
    df_new = pd.DataFrame(data)

    # If the CSV file already exists, append the new data
    if os.path.exists(filename):
        df_existing = pd.read_csv(filename)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new

    # Save combined data back to CSV
    df_combined.to_csv(filename, index=False)
    print(f"Data saved to {filename}")


if __name__ == "__main__":
    data = collect_espn_data()
    if data:
        save_data_to_csv(data)
    else:
        print("No data collected.")
