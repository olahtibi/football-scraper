import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

TOP_5_LEAGUE_CLUBS = {
    # Premier League
    "Arsenal FC", "Aston Villa", "Bournemouth", "Brentford FC", "Brighton & Hove Albion",
    "Burnley FC", "Chelsea FC", "Crystal Palace", "Everton FC", "Fulham", "Leeds United",
    "Liverpool FC", "Manchester City", "Manchester United", "Newcastle United",
    "Nottingham Forest", "Sunderland", "Tottenham Hotspur", "West Ham United",
    "Wolverhampton Wanderers",
    # La Liga
    "Alavés", "Athletic Bilbao", "Atlético de Madrid", "FC Barcelona", "Celta de Vigo",
    "Elche", "Espanyol", "Getafe", "Girona", "Levante", "Mallorca", "Osasuna",
    "Rayo Vallecano", "Real Betis Balompié", "Real Madrid", "Real Oviedo", "Real Sociedad",
    "Sevilla", "Valencia", "Villarreal CF",
    # Serie A
    "Atalanta", "Bologna", "Cagliari", "Como", "Cremonese", "ACF Fiorentina", "Genoa",
    "AC Milan", "Inter Milan", "Juventus FC", "Lazio", "Lecce", "Milan", "Napoli",
    "Parma", "Pisa", "AS Roma", "Sassuolo", "Torino", "Udinese",
    # Bundesliga
    "FC Augsburg", "1.FC Union Berlin", "Werder Bremen", "Borussia Dortmund",
    "Eintracht Frankfurt", "SC Freiburg", "Hamburger SV", "Heidenheim",
    "TSG 1899 Hoffenheim", "1. FC Köln", "RB Leipzig", "Bayer 04 Leverkusen", "1.FSV Mainz 05",
    "Borussia Mönchengladbach", "Bayern Munich", "FC St. Pauli", "VfB Stuttgart",
    "VfL Wolfsburg",
    # Ligue 1 (complete 2025–26 roster)
    "Angers", "Auxerre", "Brest", "Le Havre", "Lens", "Lille", "Lyon",
    "Olympique Marseille", "AS Monaco", "Montpellier", "Nantes", "Nice", "Paris Saint-Germain",
    "Reims", "Stade Rennais FC", "Saint‑Étienne", "Strasbourg", "Toulouse",
    "Lorient", "Paris FC", "Metz",
}

def convert_market_value(value_str):
    if not isinstance(value_str, str) or not value_str.startswith("€"):
        return 0

    value_str = value_str.replace("€", "").strip().lower()

    multiplier = 1
    if value_str.endswith("m"):
        multiplier = 1_000_000
        value_str = value_str[:-1]
    elif value_str.endswith("k"):
        multiplier = 1_000
        value_str = value_str[:-1]

    try:
        return int(float(value_str) * multiplier)
    except ValueError:
        return None

def get_country_id(country_name):
    url = "https://www.transfermarkt.com/schnellsuche/ergebnis/schnellsuche?query=" + country_name
    headers = {'User-Agent': 'Mozilla/5.0'}
    resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(resp.text, 'html.parser')
    # Look for search results linking to national teams
    for a in soup.select("a[href*='/startseite/verein/']"):
        title = a.text.strip()
        if title.lower() == country_name.lower():
            href = a['href']
            country_id = href.split("/verein/")[1].split("/")[0]
            return country_id
    raise ValueError(f"No match for country '{country_name}'")

def normalize_filename(name: str):
    return name.lower().replace(" ", "_")

def scrape_country_players(country_name, name_map, output_dir="data/players"):
    os.makedirs(output_dir, exist_ok=True)

    output_file = f"{output_dir}/{normalize_filename(country_name)}.csv"

    # Skip if file already exists
    if os.path.exists(output_file):
        print(f"Skipping {country_name}, file exists.")
        return None

    headers = {"User-Agent": "Mozilla/5.0 (compatible)"}

    # Override name if mapping exists
    tfname = name_map.get(country_name, country_name)

    country_url = tfname.replace(" ", "_")
    country_id = get_country_id(tfname)

    search_url = f"https://www.transfermarkt.com/{country_url}/startseite/verein/{country_id}"

    resp = requests.get(search_url, headers=headers)
    soup = BeautifulSoup(resp.content, "html.parser")

    players = []
    rows = soup.select("table.items > tbody > tr")

    for row in rows:

        if not row.select_one("td.hauptlink a"):
            continue

        # Player name
        name_tag = row.select_one("td.hauptlink a")
        player_name = name_tag.text.strip() if name_tag else None

        # Age
        age_tag = row.select_one("td.zentriert:nth-of-type(3)")
        player_age = None
        if age_tag and "(" in age_tag.text:
            player_age = age_tag.text.split("(")[-1].replace(")", "").strip()

        # Club
        club_tag = row.select_one("td.zentriert a[title]")
        player_club = club_tag["title"].strip() if club_tag else None

        # Top 5 league
        in_top_5 = player_club in TOP_5_LEAGUE_CLUBS

        # Market value
        mv_tag = row.select_one("td.rechts.hauptlink a")
        market_value = mv_tag.text.strip() if mv_tag else None

        players.append({
            "country": country_name,
            "player_name": player_name,
            "player_club": player_club,
            "in_top_5_league": in_top_5,
            "player_age": player_age,
            "market_value": market_value
        })

    df = pd.DataFrame(players)

    df["market_value_int"] = df["market_value"].apply(convert_market_value)

    df.to_csv(output_file, index=False)

    print(f"Saved {country_name} -> {output_file}")
