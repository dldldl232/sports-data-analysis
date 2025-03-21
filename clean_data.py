import pandas as pd
import numpy as np
import re


def remove_duplicates(input_file="collected_espn_data_player_include.csv",
                      output_file="cleaned_espn_data.csv"):
    """
    Loads the CSV, removes duplicates based on 'event_id' and 'collection_timestamp',
    and saves the cleaned data to a new CSV file.
    """
    df = pd.read_csv(input_file)
    # Drop duplicates based on event_id and collection_timestamp
    df_clean = df.drop_duplicates(subset=["event_id", "collection_timestamp"])
    df_clean.to_csv(output_file, index=False)
    print(f"Duplicates removed. Cleaned data saved to {output_file}")
    return df_clean


def extract_player_stat(leader_str):
    """
    Extracts the player name and stat from a leader string of the form "Name: Stat".
    Returns (player_name, stat) or (None, None) if not extractable.
    """
    if pd.isna(leader_str) or leader_str.strip() == "":
        return None, None
    parts = leader_str.split(":")
    if len(parts) == 2:
        name = parts[0].strip()
        try:
            stat = float(parts[1].strip())
        except ValueError:
            stat = np.nan
        return name, stat
    return None, None


def process_leader_column(df, column_name):
    """
    Processes a leader column by extracting player names and stats,
    then aggregates the number of appearances and average stat for each player.
    """
    records = []
    for value in df[column_name]:
        player, stat = extract_player_stat(value)
        if player:
            records.append((player, stat))
    # If there are no records, return an empty DataFrame
    if not records:
        return pd.DataFrame(columns=["player", "count", "avg_stat"])
    leader_df = pd.DataFrame(records, columns=["player", "stat"])
    agg_df = leader_df.groupby("player").agg(
        count=("player", "count"),
        avg_stat=("stat", "mean")
    ).reset_index()
    return agg_df


def analyze_player_leaders(clean_df):
    """
    Processes leader columns for both teams and prints out overall aggregated results.
    """
    # Process top scorers for team1 and team2
    team1_scorers = process_leader_column(clean_df, "team1_top_scorer")
    team2_scorers = process_leader_column(clean_df, "team2_top_scorer")
    all_scorers = pd.concat([team1_scorers, team2_scorers])
    all_scorers = all_scorers.groupby("player").agg(
        total_count=("count", "sum"),
        overall_avg_stat=("avg_stat", "mean")
    ).reset_index()
    print("Overall Top Scorers:")
    print(all_scorers.sort_values("total_count", ascending=False).head(10))

    # Process top rebounders
    team1_rebounds = process_leader_column(clean_df, "team1_top_rebounder")
    team2_rebounds = process_leader_column(clean_df, "team2_top_rebounder")
    all_rebounds = pd.concat([team1_rebounds, team2_rebounds])
    all_rebounds = all_rebounds.groupby("player").agg(
        total_count=("count", "sum"),
        overall_avg_stat=("avg_stat", "mean")
    ).reset_index()
    print("\nOverall Top Rebounders:")
    print(all_rebounds.sort_values("total_count", ascending=False).head(10))

    # Process top assist providers
    team1_assists = process_leader_column(clean_df, "team1_top_assist")
    team2_assists = process_leader_column(clean_df, "team2_top_assist")
    all_assists = pd.concat([team1_assists, team2_assists])
    all_assists = all_assists.groupby("player").agg(
        total_count=("count", "sum"),
        overall_avg_stat=("avg_stat", "mean")
    ).reset_index()
    print("\nOverall Top Assist Providers:")
    print(all_assists.sort_values("total_count", ascending=False).head(10))


if __name__ == "__main__":
    # First, remove duplicates from your raw data and create a cleaned CSV file.
    clean_df = remove_duplicates()

    # Now, perform player analysis on the cleaned data.
    analyze_player_leaders(clean_df)
