import json
import psycopg2
from psycopg2.extras import execute_values
import glob
import os

# Database configuration
DB_PARAMS = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "hockey",
    "port": 5432
}

def get_latest_json(directory):
    # Find all json files in the data directory and pick the newest one
    files = glob.glob(os.path.join(directory, "*.json"))
    if not files:
        return None
    return max(files, key=os.path.getctime)

def transform_player_data(player_dict):
    # Map API keys to database columns
    # Using .get() ensures the script doesn't break if a key is missing
    return (
        player_dict.get('id'),
        player_dict.get('firstName', {}).get('default'),
        player_dict.get('lastName', {}).get('default'),
        player_dict.get('sweaterNumber'),
        player_dict.get('positionCode'),
        player_dict.get('shootsCatches'),
        player_dict.get('heightInInches'),
        player_dict.get('weightInPounds'),
        player_dict.get('birthDate'),
        player_dict.get('birthCity', {}).get('default'),
        player_dict.get('birthCountry'),
        player_dict.get('headshot')
    )

def main():
    latest_file = get_latest_json('data')
    if not latest_file:
        print("No JSON files found in /data. Run fetch_roster.py first.")
        return

    print(f"Loading data from: {latest_file}")

    with open(latest_file, 'r') as f:
        raw_data = json.load(f)
    
    # Handle both dictionary (nested) and list (flat) JSON formats
    if isinstance(raw_data, dict):
        all_players_raw = []
        for group in raw_data.values():
            if isinstance(group, list):
                all_players_raw.extend(group)
    else:
        all_players_raw = raw_data

    # Convert raw JSON objects into tuples for Postgres
    processed_players = [transform_player_data(p) for p in all_players_raw]

    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()

        # Upsert logic: insert new players, update existing ones if they changed
        query = """
            INSERT INTO la_kings_roster (
                player_id, first_name, last_name, sweater_number, 
                position_code, shoots_catches, height_in_inches, 
                weight_in_pounds, birth_date, birth_city, 
                birth_country, headshot_url
            ) VALUES %s
            ON CONFLICT (player_id) DO UPDATE SET
                sweater_number = EXCLUDED.sweater_number,
                weight_in_pounds = EXCLUDED.weight_in_pounds,
                updated_at = CURRENT_TIMESTAMP;
        """

        # Efficient batch insert
        execute_values(cur, query, processed_players)
        
        conn.commit()
        print(f"Successfully loaded {len(processed_players)} players.")

    except Exception as e:
        print(f"Database error: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    main()