# sync_to_firestore.py
import pandas as pd
from google.cloud import firestore
import os
import sys
import re

def find_latest_season_folder(path):
    """
    Finds the folder for the latest season in the 'data' directory.
    """
    if not os.path.exists(path):
        print(f"Base path not found: {path}")
        return None
        
    season_folders = [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d)) and re.match(r'\d{4}-\d{4}', d)]
    if not season_folders:
        return None
    
    season_folders.sort()
    return season_folders[-1]

def sync_csv_to_firestore(file_path, gameweek_name):
    """
    Reads a CSV file and syncs its content to a subcollection
    under the appropriate gameweek document.
    """
    if not os.path.exists(file_path):
        print(f"File not found during sync: {file_path}. Skipping.")
        return

    # The subcollection will be named after the file, e.g., 'players'
    subcollection_name = os.path.splitext(os.path.basename(file_path))[0]
    
    print(f"Starting sync for '{file_path}' to subcollection '{subcollection_name}' under doc '{gameweek_name}'...")

    db = firestore.Client()
    # Path to the new subcollection: gameweeks/{GW_NAME}/{subcollection_name}
    subcollection_ref = db.collection('gameweeks').document(gameweek_name).collection(subcollection_name)

    df = pd.read_csv(file_path)
    df = df.astype(object).where(pd.notnull(df), None)

    # Clear the existing subcollection before upload
    batch = db.batch()
    delete_count = 0
    for doc in subcollection_ref.stream():
        batch.delete(doc.reference)
        delete_count += 1
        if delete_count % 499 == 0:
            batch.commit()
            batch = db.batch()
    batch.commit()
    print(f"Cleared {delete_count} existing documents in '{subcollection_name}'.")

    # Upload new data
    batch = db.batch()
    records = df.to_dict('records')
    
    for i, record in enumerate(records):
        doc_ref = subcollection_ref.document() # Let Firestore create IDs
        batch.set(doc_ref, record)
        # Commit batch every 499 operations to stay under the 500 limit
        if (i + 1) % 499 == 0:
            batch.commit()
            batch = db.batch()
            
    batch.commit() # Commit any remaining records
    print(f"✅ Successfully synced {len(records)} records to '{subcollection_name}'.")

if __name__ == "__main__":
    # 1. Find the latest season
    base_data_path = 'data'
    latest_season = find_latest_season_folder(base_data_path)
    
    if not latest_season:
        print("Error: Could not determine the latest season folder.")
        sys.exit(1)
    
    print(f"Found latest season: {latest_season}")

    # 2. Find the latest gameweek folder (highest GW number)
    gameweek_base_path = os.path.join(base_data_path, latest_season, 'By Gameweek')
    if not os.path.isdir(gameweek_base_path):
        print(f"Error: 'By Gameweek' directory not found at '{gameweek_base_path}'")
        sys.exit(1)

    gameweek_folders = [gw for gw in os.listdir(gameweek_base_path) if os.path.isdir(os.path.join(gameweek_base_path, gw))]
    if not gameweek_folders:
        print("Error: No gameweek folders found.")
        sys.exit(1)

    # Find the latest gameweek that has finished matches
    gameweek_folders.sort(key=lambda x: int(re.search(r'\d+', x).group()))
    latest_gw = None
    for gw in reversed(gameweek_folders):
        matches_file = os.path.join(gameweek_base_path, gw, 'matches.csv')
        if os.path.exists(matches_file):
            matches_df = pd.read_csv(matches_file)
            if matches_df['finished'].any():
                latest_gw = gw
                break
    if not latest_gw:
        print("Error: No gameweek with finished matches found.")
        sys.exit(1)
    print(f"Found {len(gameweek_folders)} gameweek folders. Syncing latest with finished matches: {latest_gw}")

    # 3. Sync only the latest gameweek
    current_path = os.path.join(gameweek_base_path, latest_gw)
    print(f"\n--- Processing Gameweek: {latest_gw} ---")

    for filename in os.listdir(current_path):
        if filename.endswith('.csv'):
            full_file_path = os.path.join(current_path, filename)
            sync_csv_to_firestore(full_file_path, latest_gw)
