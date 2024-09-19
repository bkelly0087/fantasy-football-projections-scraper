import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import os

CACHE_FILE = 'nfl_players_cache.csv'
CACHE_EXPIRY_DAYS = 7

def load_or_fetch_players(file_path):
    if os.path.exists(file_path):
        players_df = pd.read_csv(file_path)
        cache_date = datetime.strptime(players_df['cache_date'].iloc[0], '%Y-%m-%d')
        if datetime.now() - cache_date < timedelta(days=CACHE_EXPIRY_DAYS):
            print("Using cached player data")
            return players_df.set_index('player_id')
    
    print("Fetching fresh player data from API")
    base_url = "https://api.sleeper.app/v1"
    response = requests.get(f"{base_url}/players/nfl")
    players = response.json()
    
    players_data = []
    for player_id, player_info in players.items():
        fantasy_positions = player_info.get('fantasy_positions', [])
        if isinstance(fantasy_positions, list):
            fantasy_positions_str = ','.join(fantasy_positions)
        else:
            fantasy_positions_str = str(fantasy_positions)
        
        player_data = {
            'player_id': player_id,
            'full_name': f"{player_info.get('first_name', '')} {player_info.get('last_name', '')}".strip(),
            'first_name': player_info.get('first_name'),
            'last_name': player_info.get('last_name'),
            'age': player_info.get('age'),
            'team': player_info.get('team'),
            'position': player_info.get('position'),
            'fantasy_positions': fantasy_positions_str,
            'depth_chart_position': player_info.get('depth_chart_position'),
            'depth_chart_order': player_info.get('depth_chart_order'),
            'status': player_info.get('status'),
            'number': player_info.get('number'),
            'height': player_info.get('height'),
            'weight': player_info.get('weight'),
            'college': player_info.get('college'),
            'years_exp': player_info.get('years_exp'),
            'injury_status': player_info.get('injury_status'),
            'cache_date': datetime.now().strftime('%Y-%m-%d')
        }
        players_data.append(player_data)
    
    players_df = pd.DataFrame(players_data)
    players_df.to_csv(file_path, index=False)
    return players_df.set_index('player_id')

def fetch_player_data(player_id, season):
    base_url = "https://api.sleeper.com"
    stats_url = f"{base_url}/stats/nfl/player/{player_id}?season_type=regular&season={season}&grouping=week"
    proj_url = f"{base_url}/projections/nfl/player/{player_id}?season_type=regular&season={season}&grouping=week"
    
    stats_response = requests.get(stats_url)
    proj_response = requests.get(proj_url)
    
    return stats_response.json() if stats_response.status_code == 200 else None, \
           proj_response.json() if proj_response.status_code == 200 else None

def process_player_projection(player_info, week_projection):
    return {
        'Player': player_info['full_name'],
        'First Name': player_info['first_name'],
        'Last Name': player_info['last_name'],
        'Age': player_info['age'],
        'Position': player_info['position'],
        'Fantasy Positions': player_info['fantasy_positions'],
        'Team': week_projection['team'],
        'Depth Chart Position': player_info['depth_chart_position'],
        'Depth Chart Order': player_info['depth_chart_order'],
        'Status': player_info['status'],
        'Number': player_info['number'],
        'Height': player_info['height'],
        'Weight': player_info['weight'],
        'College': player_info['college'],
        'Years Experience': player_info['years_exp'],
        'Injury Status': player_info['injury_status'],
        'Opponent': week_projection['opponent'],
        'Date': week_projection['date'],
        'ADP (Half PPR)': week_projection['stats'].get('adp_dd_ppr', 0),
        'Pos ADP (Half PPR)': week_projection['stats'].get('pos_adp_dd_ppr', 0),
        'Projected Points (Half PPR)': week_projection['stats'].get('pts_half_ppr', 0),
        'Projected Points (PPR)': week_projection['stats'].get('pts_ppr', 0),
        'Projected Points (Standard)': week_projection['stats'].get('pts_std', 0),
        'Proj Pass Attempts': week_projection['stats'].get('pass_att', 0),
        'Proj Pass Completions': week_projection['stats'].get('pass_cmp', 0),
        'Proj Pass Yards': week_projection['stats'].get('pass_yd', 0),
        'Proj Pass TDs': week_projection['stats'].get('pass_td', 0),
        'Proj Pass INTs': week_projection['stats'].get('pass_int', 0),
        'Proj Rush Attempts': week_projection['stats'].get('rush_att', 0),
        'Proj Rush Yards': week_projection['stats'].get('rush_yd', 0),
        'Proj Rush TDs': week_projection['stats'].get('rush_td', 0),
        'Proj Fumbles': week_projection['stats'].get('fum', 0),
        'Proj Fumbles Lost': week_projection['stats'].get('fum_lost', 0),
        'Data Source': week_projection['company'],
        'Last Updated': datetime.fromtimestamp(week_projection['updated_at']/1000).strftime('%Y-%m-%d %H:%M:%S'),
    }

def get_sleeper_data(week, season=2024):
    players_df = load_or_fetch_players(CACHE_FILE)
    player_data = []

#    for player_id, player_info in players_df.iterrows():
#        player_stats, player_projections = fetch_player_data(player_id, season)
#        
#        if player_stats and player_projections and str(week) in player_projections:
#            week_projection = player_projections[str(week)]
#           player_data.append(process_player_projection(player_info, week_projection))
#    
#    return pd.DataFrame(player_data)
    return players_df


def get_fantasypros_projections(week):
    url = f"https://www.fantasypros.com/nfl/projections/qb.php?week={week}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    data = []
    table = soup.find('table', {'id': 'data'})
    for row in table.find_all('tr')[1:]:
        cols = row.find_all('td')
        if len(cols) > 0:
            player = cols[0].text.strip()
            points = cols[-1].text.strip()
            data.append({'Player': player, 'Projected Points': points})
    return pd.DataFrame(data)

def get_espn_projections(week):
    # Placeholder for ESPN API integration
    pass

def get_yahoo_projections(week):
    # Placeholder for Yahoo API integration
    pass

def main():
    week = 1  # You can change this or make it a user input
    projections = {}
    
    projections['Sleeper'] = get_sleeper_data(week)
    print(projections['Sleeper'].head())
    #projections['FantasyPros'] = get_fantasypros_projections(week)
    # Uncomment and implement these as you add more sources
    # projections['ESPN'] = get_espn_projections(week)
    # projections['Yahoo'] = get_yahoo_projections(week)
    
    # Save or display the results
    for source, df in projections.items():
        print(f"Data from {source}:")
        print(df.head())
        df.to_csv(f"{source}_data_week_{week}.csv", index=False)

if __name__ == "__main__":
    main()