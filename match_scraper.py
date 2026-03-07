import pandas as pd
from datetime import datetime, timedelta

def process_matches(input_file: str, output_file: str = "data/processed-results.csv"):
    """
    Process international football results dataset.

    Args:
        input_file (str): Path to raw CSV file
        output_file (str): Path to output processed CSV file
    """

    # Load dataset
    df = pd.read_csv(input_file)

    # Parse date column
    df['date'] = pd.to_datetime(df['date'])

    # Filter last 5 years
    today = datetime.today()
    cutoff_date = today - timedelta(days=5 * 365)
    df = df[df['date'] >= cutoff_date]

    # Create required fields
    df['venue'] = ""
    df['location'] = df['city'].fillna("") + "," + df['country'].fillna("")
    df['competition'] = df['tournament']

    # Rename and select columns
    df = df.rename(columns={"date": "event_date"})

    df_final = df[
        [
            "away_team",
            "home_team",
            "home_score",
            "away_score",
            "event_date",
            "venue",
            "location",
            "competition",
        ]
    ]

    # Sort by date
    df_final = df_final.sort_values("event_date")

    # Save output
    df_final.to_csv(output_file, index=False)

    print(f"Processed dataset saved to {output_file}")

__all__ = ["process_matches"]
