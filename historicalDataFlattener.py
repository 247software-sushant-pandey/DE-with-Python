import json
import threading
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson.json_util import dumps
import pandas as pd
from pandas import json_normalize
import os
from datetime import datetime

# MongoDB connection setup
def connect_to_mongo(uri, db_name, coll_name):
    client = MongoClient(uri)
    db = client[db_name]
    coll = db[coll_name]
    return coll

# Flatten logic that dynamically handles nested lists and dicts
def flatten_doc_dynamic(doc):
    try:
        flat = json_normalize(doc, max_level=6)
        return flat
    except Exception as e:
        print(f"Flattening error: {e}")
        return pd.DataFrame()

# Export complete DataFrame to a single JSON file
def export_to_json(df, file_path):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df.to_json(file_path, orient="records", lines=True, force_ascii=False)
        print(f"Flattened data written to {file_path}")
    except Exception as e:
        print(f"Export error: {e}")

# Process historical documents first
def process_historical_data(uri, db_name, coll_name):
    coll = connect_to_mongo(uri, db_name, coll_name)
    print("Processing historical data...")
    all_docs = []
    try:
        cursor = coll.find()
        for doc in cursor:
            df = flatten_doc_dynamic(doc)
            if not df.empty:
                all_docs.append(df)
    except PyMongoError as e:
        print(f"MongoDB historical read error: {e}")
    return pd.concat(all_docs, ignore_index=True) if all_docs else pd.DataFrame()

# CDC Listener with automatic flattening and saving to JSON
# Appends CDC changes to the existing JSON file
def start_mongo_stream(uri, db_name, coll_name, json_path):
    coll = connect_to_mongo(uri, db_name, coll_name)

    def stream():
        print("Starting MongoDB CDC stream...")
        try:
            with coll.watch(full_document='updateLookup') as stream:
                for change in stream:
                    doc = change.get("fullDocument")
                    if doc:
                        df = flatten_doc_dynamic(doc)
                        if not df.empty:
                            try:
                                # Append new rows to JSON file
                                with open(json_path, "a", encoding="utf-8") as f:
                                    df.to_json(f, orient="records", lines=True, force_ascii=False)
                            except Exception as e:
                                print(f"Error appending to JSON: {e}")
        except PyMongoError as e:
            print(f"MongoDB stream error: {e}")

    thread = threading.Thread(target=stream, daemon=True)
    thread.start()

# Entry point for full pipeline: historical + CDC
def flatten_streamed_json(uri, db_name, coll_name, out_dir="./flattened_data"):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_file = os.path.join(out_dir, f"flattened_{coll_name}___{timestamp}.json")

    df = process_historical_data(uri, db_name, coll_name)
    if not df.empty:
        export_to_json(df, json_file)

    #start_mongo_stream(uri, db_name, coll_name, json_file)

# Example Usage (uncomment to test)
if __name__ == "__main__":
    MONGO_URI = "MONGODB_URI"
    DB_NAME = "DATABASE"
    COLLECTION_NAME = [
        "COLLECTION 1","COLLECTION 2" 
        ]
    for coll in COLLECTION_NAME:
        print(f"Processing collection: {coll}")
        # Flatten and stream the collection
        flatten_streamed_json(MONGO_URI, DB_NAME, coll)
