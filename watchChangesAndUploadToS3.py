# This script watches specified MongoDB collections for changes and uploads change data capture (CDC) events to an AWS S3 bucket when there is no activity for 1 minute.
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import threading
import json
import time
import boto3
import os
from datetime import datetime

# MongoDB connection
client = MongoClient("MONGODB_URI")  # Replace with your actual URI
db = client["DATABASE NAME"]  # Replace with your actual database name

# List of collections to watch
collections_to_watch = [
    "Collections"
]

bucket_name = input("Enter S3 bucket name: ")
s3_folder = input("Enter S3 folder path (e.g. myfolder/subfolder): ")

for col in collections_to_watch:
    print(f"Watching collection: {col}")

# Define the pipeline to filter changes for specified collections
pipeline = [{'$match': {'ns.coll': {'$in': collections_to_watch}}}]

# Globals
cdc_events = []
last_activity_time = time.time()
lock = threading.Lock()
output_file = "cdc_events.json"

# AWS S3 client
s3_client = boto3.client("s3")

def upload_to_s3():
    """Uploads the local JSON file to S3 with a timestamped filename."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{s3_folder}/cdc_events_{timestamp}.json"

    try:
        s3_client.upload_file(output_file, bucket_name, s3_key)
        print(f"Uploaded {output_file} to s3://{bucket_name}/{s3_key}")
        os.remove(output_file)  # Clean up local file
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def watch_database():
    global last_activity_time

    try:
        with db.watch(pipeline) as stream:
            for change in stream:
                namespace = change["ns"]
                collection_name = namespace["coll"]

                print(f"\n[Change in {collection_name}]")
                print(change)

                # Append CDC event
                with lock:
                    cdc_events.append(change)
                    last_activity_time = time.time()

                # Save events to local JSON file continuously
                with open(output_file, "w") as f:
                    json.dump(cdc_events, f, default=str, indent=2)

    except PyMongoError as e:
        print(f"Error watching database: {e}")

def inactivity_monitor():
    """Checks for 1 minute of inactivity and pushes file to S3."""
    global cdc_events, last_activity_time

    while True:
        time.sleep(10)  # check every 10s
        with lock:
            if cdc_events and (time.time() - last_activity_time > 60):
                print("No activity detected for 1 min. Uploading file to S3...")
                upload_to_s3()
                cdc_events = []  # Reset after upload

# Start watcher thread
watcher_thread = threading.Thread(target=watch_database, daemon=True)
watcher_thread.start()

# Start inactivity monitor thread
monitor_thread = threading.Thread(target=inactivity_monitor, daemon=True)
monitor_thread.start()

# Keep main thread alive
watcher_thread.join()
