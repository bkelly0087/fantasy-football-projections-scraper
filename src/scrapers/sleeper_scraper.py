import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import os
import time

MAX_REQUESTS_PER_MINUTE = 500
REQUEST_INTERVAL = 60 / MAX_REQUESTS_PER_MINUTE
CACHE_EXPIRY_DAYS = 7

# Construct the full path to the NFL players cache file
current_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(current_dir, '..', '..', 'data')
CACHE_FILE = os.path.join(data_dir, 'nfl_players_cache.csv')



def load_or_fetch_players(cache_file, force_refresh=False):
    try:
        if not force_refresh:
            players_df = pd.read_csv(cache_file)
            players_df['player_id'] = players_df['player_id'].astype(str)  # Ensure player_id is string
            players_df.set_index('player_id', inplace=True)
            cache_date = datetime.strptime(players_df['cache_date'].iloc[0], '%Y-%m-%d')
            if datetime.now() - cache_date < timedelta(days=7):  # Cache is less than 7 days old
                print("Using cached player data")
                return players_df
    except (FileNotFoundError, KeyError, ValueError):
        pass  # If any error occurs, we'll fetch fresh data

    print("Fetching fresh player data from API")
    response = requests.get("https://api.sleeper.app/v1/players/nfl")
    if response.status_code != 200:
        raise Exception("Failed to fetch player data from Sleeper API")

    players = response.json()
    players_data = []

    for player_id, player_info in players.items():
        fantasy_positions = player_info.get('fantasy_positions', [])
        if not isinstance(fantasy_positions, list):
            fantasy_positions = []

        player_data = {
            'player_id': player_id,
            'full_name': player_info.get('full_name'),
            'first_name': player_info.get('first_name'),
            'last_name': player_info.get('last_name'),
            'age': player_info.get('age'),
            'position': player_info.get('position'),
            'fantasy_positions': ','.join(fantasy_positions),
            'team': player_info.get('team'),
            'status': player_info.get('status'),
            'injury_status': player_info.get('injury_status'),
            'depth_chart_position': player_info.get('depth_chart_position'),
            'depth_chart_order': player_info.get('depth_chart_order'),
            'years_exp': player_info.get('years_exp'),
            'height': player_info.get('height'),
            'weight': player_info.get('weight'),
            'college': player_info.get('college'),
            'number': player_info.get('number'),
            'birth_date': player_info.get('birth_date'),
            'active': player_info.get('active'),
            'espn_id': player_info.get('espn_id'),
            'yahoo_id': player_info.get('yahoo_id'),
            'sportradar_id': player_info.get('sportradar_id'),
            'rotowire_id': player_info.get('rotowire_id'),
            'swish_id': player_info.get('swish_id'),
            'pandascore_id': player_info.get('pandascore_id'),
            'gsis_id': player_info.get('gsis_id'),
            'fantasy_data_id': player_info.get('fantasy_data_id'),
            'rotoworld_id': player_info.get('rotoworld_id'),
            'cache_date': datetime.now().strftime('%Y-%m-%d')
        }
        players_data.append(player_data)

    players_df = pd.DataFrame(players_data)
    players_df['player_id'] = players_df['player_id'].astype(str)  # Ensure player_id is string
    players_df.set_index('player_id', inplace=True)
    players_df.to_csv(cache_file)
    print(f"Player data cached to {cache_file}")
    return players_df

def filter_active_players(players_df):
    fantasy_positions = ['QB', 'RB', 'WR', 'TE', 'K', 'DEF']
    
    return players_df[
        (players_df['active'] == True) & 
        (players_df['team'].notna()) & 
        (players_df['position'].isin(fantasy_positions))
    ]

def rate_limited_request(url):
    """Make a rate-limited request to the API"""
    response = requests.get(url)
    time.sleep(REQUEST_INTERVAL)
    return response

def fetch_player_data(player_id, season):
    base_url = "https://api.sleeper.com"
    stats_url = f"{base_url}/stats/nfl/player/{player_id}?season_type=regular&season={season}&grouping=week"
    proj_url = f"{base_url}/projections/nfl/player/{player_id}?season_type=regular&season={season}&grouping=week"
    
    stats_response = rate_limited_request(stats_url)
    proj_response = rate_limited_request(proj_url)
    
    combined_data = {}
    
    if stats_response.status_code == 200:
        stats_data = stats_response.json()
        for week, week_data in stats_data.items():
            if week_data and week_data.get('stats'):  # Check if week_data is not None and has stats
                combined_data[week] = {'stats': week_data}
    
    if proj_response.status_code == 200:
        proj_data = proj_response.json()
        for week, week_data in proj_data.items():
            if week_data and week_data.get('stats') and week in combined_data:  # Check if week_data is not None and has stats
                combined_data[week]['projections'] = week_data
    
    return combined_data if combined_data else None


def process_player_data(player_id, player_info, week_data, week):
    stats = week_data.get('stats', {}).get('stats', {})
    projections = week_data.get('projections', {}).get('stats', {})
    
    return {
        'player_id': player_id,
        'player_name': player_info['full_name'],
        'age': player_info['age'],
        'position': player_info['position'],
        'depth_chart_order': player_info['depth_chart_order'],
        'week': week,
        'team': week_data['stats'].get('team'),
        'opponent': week_data['stats'].get('opponent'),
        'date': week_data['stats'].get('date'),
        'projected_points_half_ppr': projections.get('pts_half_ppr', 'N/A'),
        'actual_points_half_ppr': stats.get('pts_half_ppr', 0),
        'projected_pass_yards': projections.get('pass_yd', 'N/A'),
        'actual_pass_yards': stats.get('pass_yd', 0),
        'projected_pass_tds': projections.get('pass_td', 'N/A'),
        'actual_pass_tds': stats.get('pass_td', 0),
        'projected_pass_attempts': projections.get('pass_att', 'N/A'),
        'actual_pass_attempts': stats.get('pass_att', 0),
        'projected_pass_completions': projections.get('pass_cmp', 'N/A'),
        'actual_pass_completions': stats.get('pass_cmp', 0),
        'projected_rush_yards': projections.get('rush_yd', 'N/A'),
        'actual_rush_yards': stats.get('rush_yd', 0),
        'projected_rush_attempts': projections.get('rush_att', 'N/A'),
        'actual_rush_attempts': stats.get('rush_att', 0),
        'projected_rush_tds': projections.get('rush_td', 'N/A'),
        'actual_rush_tds': stats.get('rush_td', 0),
        'actual_interceptions': stats.get('pass_int', 0),
        'actual_sacks': stats.get('pass_sack', 0),
        'actual_fumbles': stats.get('fum', 0),
        'actual_fumbles_lost': stats.get('fum_lost', 0),
    }

def get_player_info(player_id, players_df):
    if player_id in players_df.index:
        return players_df.loc[player_id]
    else:
        print(f"Player (ID: {player_id}) not found in player cache")
        return None

def get_sleeper_data(player_id, season, player_info):
    if player_info is not None:
        player_data = fetch_player_data(player_id, season)
        
        processed_data = []
        if player_data:
            for week, week_data in player_data.items():
                if 'stats' in week_data or 'projections' in week_data:
                    processed_week_data = process_player_data(player_id, player_info, week_data, week)
                    processed_data.append(processed_week_data)
        
        return pd.DataFrame(processed_data)
    else:
        return pd.DataFrame()


def main():
    players_df = load_or_fetch_players(CACHE_FILE)
    active_players_df = filter_active_players(players_df)
    player_ids = active_players_df.index.tolist()
    
    current_year = 2023
    season = current_year

    all_player_data = []

    for player_id in player_ids:
        player_info = get_player_info(player_id, players_df)
        if player_info is not None:
            sleeper_data = get_sleeper_data(player_id, season, player_info)
            if not sleeper_data.empty:
                all_player_data.append(sleeper_data)
                print(f"Data processed for player {player_id}")
            else:
                print(f"No data retrieved for player {player_id}")
        else:
            print(f"Skipping player {player_id} due to missing info")

    if all_player_data:
        combined_data = pd.concat(all_player_data, ignore_index=True)
        filename = f"data/Sleeper_data_all_players_season_{season}.csv"
        combined_data.to_csv(filename, index=False)
        print(f"Combined data for all players saved to {filename}")
        print(f"Total rows of data: {len(combined_data)}")
    else:
        print("No data retrieved for any players")

if __name__ == "__main__":
    main()