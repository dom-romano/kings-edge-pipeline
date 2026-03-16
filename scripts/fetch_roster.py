import json
import os
from nhlpy import NHLClient

# Basic usage
client = NHLClient()

roster = client.teams.team_roster(team_abbr="LAK", season="20252026")

# Ensure the data directory exists
os.makedirs('data', exist_ok=True)

file_path = 'data/raw_roster.json'
with open(file_path, 'w') as f:
    # This saves the whole dictionary (forwards, defense, goalies) in one valid file
    # add to list of players
    players = roster['forwards'] + roster['defensemen'] + roster['goalies']
    json.dump(players, f, indent=4)