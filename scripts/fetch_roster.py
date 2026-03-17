import json
import os
from nhlpy import NHLClient
import time

# Basic usage
client = NHLClient()

roster = client.teams.team_roster(team_abbr="LAK", season="20252026")

# Ensure the data directory exists
os.makedirs('data', exist_ok=True)

# save file with date and time to track roster changes over time, and to avoid overwriting previous files
file_path = f'data/raw_roster_{time.strftime("%Y-%m-%d_%H-%M-%S")}.json'
with open(file_path, 'w') as f:
    # add to list of players
    players = roster['forwards'] + roster['defensemen'] + roster['goalies']
    json.dump(players, f, indent=4)