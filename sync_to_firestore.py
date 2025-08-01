# sync_to_firestore.py
import pandas as pd
from google.cloud import firestore
import os
import sys

# List of CSV files to sync. The script will create a collection with the same name as the file base name.
# e.g., 'elo_ratings.csv' will be uploaded to the 'elo_ratings' collection.
FILES_TO_SYNC = ['data/elo_ratings.csv', 'data/team_ratings.csv', 'data/upcoming_fixtures.csv']

def sync_csv_to_firestore(file_path):
    """
    Reads a CSV file and syncs its content to a Firestore collection.
    The collection is named after the CSV file's base name.
    """
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}. Skipping.")
        return

    collection_name = os.path.splitext(os.path.basename(file_path))[0]
    print(f"Starting sync for '{file_path}' to Firestore collection '{collection_name}'...")

    # Initialize Firestore DB client
    db = firestore.Client()
    collection_ref = db.collection(collection_name)

    # Use pandas to read CSV
    df = pd.read_csv(file_path)
    df = df.astype(object).where(pd.notnull(df), None) # Replace NaN with None for Firestore compatibility

    # Use a batch writer for efficient uploads
    batch = db.batch()
    
    # Clear the existing collection first (optional, but ensures a clean sync)
    # Note: Deleting collections this way is not recommended for very large collections.
    for doc in collection_ref.stream():
        batch.delete(doc.reference)
    batch.commit()
    print(f"Cleared existing documents in '{collection_name}'.")

    # Reset batch for writing new data
    batch = db.batch()
    records = df.to_dict('records')
    
    for i, record in enumerate(records):
        doc_ref = collection_ref.document() # Let Firestore auto-generate document IDs
        batch.set(doc_ref, record)
        # Firestore batch can handle a maximum of 500 operations. Commit every 499.
        if (i + 1) % 499 == 0:
            batch.commit()
            batch = db.batch() # Start a new batch
            
    batch.commit() # Commit any remaining records
    print(f"✅ Successfully synced {len(records)} records to '{collection_name}'.")


if __name__ == "__main__":
    for csv_file in FILES_TO_SYNC:
        sync_csv_to_firestore(csv_file)
