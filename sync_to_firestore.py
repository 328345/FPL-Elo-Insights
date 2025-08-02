# sync_to_firestore.py
import pandas as pd
from google.cloud import firestore
import os
import sys
import re

def find_latest_folder(path, numeric_sort=False):
    """
    Finds the latest folder in a given path, either alphabetically or numerically.
    """
    if not os.path.exists(path):
        print(f"Base path not found: {path}. Cannot find latest folder.")
        return None
        
    folders = [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]
    if not folders:
        return None

    if numeric_sort:
        # Sorts folders like 'GW1', 'GW10', 'GW2' correctly as 1, 2, 10
        folders.sort(key=lambda f: int(re.search(r'\d+', f).group()))
    else:
        folders.sort()
        
    return folders[-1]

def sync_csv_to_firestore(file_path):
    """
    Reads a CSV file and syncs its content to a Firestore collection.
    """
    if not os.path.exists(file_path):
        print(f"File not found during sync: {file_path}. Skipping.")
        return

    collection_name = os.path.splitext(os.path.basename(file_path))[0]
    print(f"Starting sync for '{file_path}' to Firestore collection '{collection_name}'...")

    db = firestore.Client()
    collection_ref = db.collection(collection_name)

    df = pd.read_csv(file_path)
    df = df.astype(object).where(pd.notnull(df), None)

    batch = db.batch()
    for doc in collection_ref.stream():
        batch.delete(doc.reference)
    batch.commit()
    print(f"Cleared existing documents in '{collection_name}'.")

    batch = db.batch()
    records = df.to_dict('records')
    
    for i, record in enumerate(records):
        doc_ref = collection_ref.document()
        batch.set(doc_ref, record)
        if (i + 1) % 499 == 0:
            batch.commit()
            batch = db.batch()
            
    batch.commit()
    print(f"✅ Successfully synced {len(records)} records to '{collection_name}'.")


if __name__ == "__main__":
    # --- Manually setting the path to only look at GW0 data ---
    final_path = 'data/2025-2026/By Gameweek/GW0'
    print(f"==> Manually using data path: {final_path}")
    
    # Check if the hardcoded path exists before proceeding
    if not os.path.isdir(final_path):
        print(f"Error: The specified path '{final_path}' does not exist.")
        sys.exit(1)

    # Define which files to sync from that folder
    files_to_sync = [
        'fixtures.csv',
        'matches.csv',
        'playermatchstats.csv',
        'players.csv',
        'playerstats.csv',
        'teams.csv'
    ]

    # Loop through and sync each file
    for filename in files_to_sync:
        full_file_path = os.path.join(final_path, filename)
        sync_csv_to_firestore(full_file_path)
