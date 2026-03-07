from config import max_retries, transfermarkt_name_map, transfermarkt_skip
from match_scraper import process_matches
from transfermarkt_scraper import scrape_country_players
import pandas as pd
import time
import random

def collect_all_teams(input_file: str):
    df = pd.read_csv(input_file)
    teams = pd.concat([
        df["home_team"].dropna(),
        df["away_team"].dropna()
    ]).unique()
    return sorted(teams)

def merge_player_files(input_dir: str = "data/players", output_file: str = "data/processed-players.csv"):
    """
    Merge all player CSV files inside input_dir into one CSV file.

    Args:
        input_dir (str): Directory containing player CSV files
        output_file (str): Path to merged output CSV file
    """

    import os

    all_files = []

    if not os.path.exists(input_dir):
        print(f"Input directory {input_dir} does not exist.")
        return

    for file in os.listdir(input_dir):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(input_dir, file))
            all_files.append(df)

    if not all_files:
        print("No CSV files found to merge.")
        return

    merged_df = pd.concat(all_files, ignore_index=True)
    merged_df.to_csv(output_file, index=False)

    print(f"Merged player dataset saved to {output_file}")

def scrape_players(country_list):
    for country in country_list:

        if country in transfermarkt_skip:
            print(f"Skipping {country}")
            continue

        success = False

        for attempt in range(1, max_retries + 1):
            # time.sleep(random.uniform(5, 10))  # Be polite before each attempt
            try:
                scrape_country_players(country, transfermarkt_name_map)
                print(f"✓ Scraped {country}")
                success = True
                break  # Exit retry loop if successful
            except Exception as e:
                print(f"⚠️ Attempt {attempt} failed for {country}: {e}")

                if attempt < max_retries:
                    wait = random.uniform(3, 6)
                    print(f"↻ Retrying {country} in {wait:.1f}s...")
                    time.sleep(wait)
                else:
                    print(f"❌ Giving up on {country} after {max_retries} attempts.")

def main():

    raw_file = "data/raw-results.csv"
    output_file = "data/processed-results.csv"
    process_matches(raw_file, output_file)
    country_list = collect_all_teams("data/processed-results.csv")
    print(f"Countries: {country_list}")
    scrape_players(country_list)
    merge_player_files()

if __name__ == "__main__":
    main()


