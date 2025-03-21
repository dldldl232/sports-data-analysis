import json

import requests
import pandas as pd
from datetime import datetime, UTC, timezone
import os
import schedule
import time

run_count = 0
def collect_espn_data():
    # ESPN API endpoint for NBA Scoreboard
    url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"

    # Fetch the data from ESPN
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch data: {response.status_code}")
        return None

    data_espn = response.json()
    events = data_espn.get("events", [])

    collected = []
    # Record the collection timestamp
    collection_time = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    # Loop over each event and extract details
    for event in events:
        event_id = event.get("id", "")
        event_name = event.get("name", "")
        event_date = event.get("date", "")

        # Initialize default values for teams and scores
        team1 = team2 = score1 = score2 = ""
        team1_leaders = {"points": "", "rebounds": "", "assists": ""}
        team2_leaders = {"points": "", "rebounds": "", "assists": ""}

        competitions = event.get("competitions", [])
        if competitions:
            competition = competitions[0]
            competitors = competition.get("competitors", [])
            if len(competitors) >= 2:
                team1 = competitors[0]["team"].get("displayName", "")
                team2 = competitors[1]["team"].get("displayName", "")
                score1 = competitors[0].get("score", "")
                score2 = competitors[1].get("score", "")

                # Extract leaders for each team
                for competitor in competitors:
                    leaders = competitor.get("leaders", [])
                    print(f"Leaders for {competitor['team']['displayName']}: {leaders}")  # Debugging print
                    for leader in leaders:
                        category = leader.get("name", "").lower()  # e.g., "points", "rebounds", "assists"
                        top_player = leader.get("leaders", [])[0] if leader.get("leaders") else None
                        if top_player:
                            player_name = top_player["athlete"].get("displayName", "")
                            player_stat = top_player.get("displayValue", "")
                            print(f"Top player in {category}: {player_name} ({player_stat})")  # Debugging print

                            if competitor["team"]["displayName"] == team1:
                                team1_leaders[category] = f"{player_name}: {player_stat}"
                            elif competitor["team"]["displayName"] == team2:
                                team2_leaders[category] = f"{player_name}: {player_stat}"

                # Append extracted data along with the collection timestamp
                collected.append({
                    "collection_timestamp": collection_time,
                    "event_id": event_id,
                    "event_name": event_name,
                    "event_date": event_date,
                    "team1": team1,
                    "score1": score1,
                    "team2": team2,
                    "score2": score2,
                    "team1_top_scorer": team1_leaders.get("points", ""),
                    "team1_top_rebounder": team1_leaders.get("rebounds", ""),
                    "team1_top_assist": team1_leaders.get("assists", ""),
                    "team2_top_scorer": team2_leaders.get("points", ""),
                    "team2_top_rebounder": team2_leaders.get("rebounds", ""),
                    "team2_top_assist": team2_leaders.get("assists", "")
                })
    print(json.dumps(collected, indent=2))

    return collected


def save_data_to_csv(data_espn, filename="collected_espn_data_player_include.csv"):
    # Create a new DataFrame from collected data
    df_new = pd.DataFrame(data_espn)

    # If the CSV file already exists, append the new data
    if os.path.exists(filename):
        df_existing = pd.read_csv(filename)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new

    # Save combined data back to CSV

    df_combined.to_csv(filename, index=False)
    print(f"Data saved to {filename}")


def collect_and_save_data():
    global run_count
    run_count += 1
    data = collect_espn_data()
    if data:
        print(f"Fetched {len(data)} events.")
        save_data_to_csv(data)
        print(f"Data saved at {datetime.now(timezone.utc)}")
    else:
        print("No data collected.")


schedule.every().hour.do(collect_and_save_data)

if __name__ == "__main__":
    # data = collect_espn_data()
    # if data:
    #     print(f"Fetched {len(data)} games.")
    #     save_data_to_csv(data)
    # else:
    #     print("No data collected.")
    collect_and_save_data()

    # Keep the script running to execute the scheduled job periodically
    while run_count <= 5:
        schedule.run_pending()
        time.sleep(1)
