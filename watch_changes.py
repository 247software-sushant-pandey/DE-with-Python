from pymongo import MongoClient
from pymongo.errors import PyMongoError
import threading

# MongoDB connection
mongoUri= "URI STRING"
client = MongoClient(mongoUri)
data=input("Enter database name: ")
db = client[data]

# List of collections to watch
csv_string = input("Enter collection names (comma-separated): ")
collections_to_watch = [item.strip() for item in csv_string.split(",")]


def watch_collection(collection_name):
    collection = db[collection_name]
    try:
        with collection.watch() as stream:
            print(f"Watching {collection_name}...")
            for change in stream:
                print(f"\n[Change in {collection_name}]")
                print(change)
    except PyMongoError as e:
        print(f"Error watching {collection_name}: {e}")

# Start a thread for each collection
threads = []
for col in collections_to_watch:
    t = threading.Thread(target=watch_collection, args=(col,))
    t.start()
    threads.append(t)

# Wait for all threads (optional if running continuously)
for t in threads:
    t.join()


