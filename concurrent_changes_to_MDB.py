from pymongo import MongoClient
from pymongo.errors import PyMongoError
import threading
import time
from datetime import datetime
import random
import string

# MongoDB connection
mongoUri= "URI"
client = MongoClient(mongoUri)
data=input("Enter database name: ")
db = client[data]

# List of collections to watch
csv_string = input("Enter collection names (comma-separated): ")
collections_to_watch = [item.strip() for item in csv_string.split(",")]

# Configuration for concurrent updates
document_id = input("Enter document ID to update simultaneously: ")
num_changes = int(input("Enter number of simultaneous changes to perform: "))

# Synchronization objects
start_barrier = threading.Barrier(num_changes)
results = []
results_lock = threading.Lock()
shared_timestamp = None
timestamp_lock = threading.Lock()

def get_document_fields(collection_name):
    """Get existing fields from the document"""
    collection = db[collection_name]
    try:
        doc = collection.find_one({"_id": document_id})
        if doc:
            # Get all fields except _id
            fields = [key for key in doc.keys() if key != '_id']
            return fields, doc
        else:
            print(f"Document with _id {document_id} not found in {collection_name}")
            return [], None
    except PyMongoError as e:
        print(f"Error fetching document from {collection_name}: {e}")
        return [], None

def generate_random_value(field_name, original_value):
    """Generate random value based on the original field type"""
    if isinstance(original_value, str):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    elif isinstance(original_value, int):
        return random.randint(1, 1000)
    elif isinstance(original_value, float):
        return round(random.uniform(1.0, 100.0), 2)
    elif isinstance(original_value, bool):
        return random.choice([True, False])
    elif isinstance(original_value, list):
        return [f"item_{random.randint(1, 100)}" for _ in range(random.randint(1, 3))]
    elif isinstance(original_value, dict):
        return {f"key_{random.randint(1, 10)}": f"value_{random.randint(1, 100)}"}
    else:
        return f"updated_{field_name}_{random.randint(1, 1000)}"

def watch_collection(collection_name):
    collection = db[collection_name]
    try:
        with collection.watch() as stream:
            print(f"Watching {collection_name}...")
            for change in stream:
                print(change)
    except PyMongoError as e:
        print(f"Error watching {collection_name}: {e}")

def simultaneous_update(collection_name, thread_id, field_to_update, new_value):
    global shared_timestamp
    collection = db[collection_name]
    
    try:
        # Wait for all threads to be ready
        print(f"Thread {thread_id} ready, waiting for others...")
        start_barrier.wait()
        
        # First thread to pass the barrier sets the shared timestamp
        with timestamp_lock:
            if shared_timestamp is None:
                shared_timestamp = datetime.now()
        
        # Use the shared timestamp for all operations
        update_data = {
            "$set": {
                field_to_update: new_value,
                f"thread_{thread_id}_timestamp": shared_timestamp,
                "last_updated": shared_timestamp
            }
        }
        
        result = collection.update_one(
            {"_id": document_id}, 
            update_data
        )
        
        end_time = datetime.now()
        
        # Store result
        with results_lock:
            results.append({
                'thread_id': thread_id,
                'collection': collection_name,
                'field': field_to_update,
                'value': new_value,
                'start_time': shared_timestamp,
                'end_time': end_time,
                'duration': (end_time - shared_timestamp).total_seconds(),
                'matched_count': result.matched_count,
                'modified_count': result.modified_count
            })
        
        print(f"Thread {thread_id}: Updated {field_to_update} = {new_value} at {shared_timestamp}")
        
    except PyMongoError as e:
        print(f"Error in thread {thread_id}: {e}")

# Get document fields from the first collection
if collections_to_watch:
    fields, original_doc = get_document_fields(collections_to_watch[0])
    
    if not fields:
        print("No fields found or document doesn't exist. Creating a sample document...")
        # Create a sample document with various field types
        sample_doc = {
            "_id": document_id,
            "name": "sample_name",
            "count": 0,
            "price": 10.5,
            "active": True,
            "tags": ["tag1", "tag2"],
            "metadata": {"created": datetime.now()}
        }
        
        collection = db[collections_to_watch[0]]
        collection.insert_one(sample_doc)
        fields, original_doc = get_document_fields(collections_to_watch[0])

# Start watching threads
watch_threads = []
for col in collections_to_watch:
    t = threading.Thread(target=watch_collection, args=(col,), daemon=True)
    t.start()
    watch_threads.append(t)

# Prepare simultaneous updates
update_threads = []
for i in range(num_changes):
    # Select a random field to update (or cycle through fields)
    field_to_update = fields[i % len(fields)] if fields else f"field_{i}"
    
    # Generate new value based on original type
    if original_doc and field_to_update in original_doc:
        new_value = generate_random_value(field_to_update, original_doc[field_to_update])
    else:
        new_value = f"concurrent_update_{i}_{random.randint(1, 1000)}"
    
    # Use the first collection for updates
    collection_name = collections_to_watch[0]
    
    t = threading.Thread(
        target=simultaneous_update,
        args=(collection_name, i, field_to_update, new_value)
    )
    update_threads.append(t)

print(f"\nPreparing {num_changes} simultaneous updates...")
print(f"Target document: {document_id}")
print(f"Available fields: {fields}")

# Start all update threads
for t in update_threads:
    t.start()

# Wait for all update threads to complete
for t in update_threads:
    t.join()

# Display results
print(f"\n{'='*60}")
print("SIMULTANEOUS UPDATE RESULTS")
print(f"{'='*60}")
print(f"Shared timestamp used: {shared_timestamp}")
print(f"{'='*60}")

for result in sorted(results, key=lambda x: x['thread_id']):
    print(f"Thread {result['thread_id']}: {result['field']} = {result['value']}")
    print(f"  Timestamp: {result['start_time']}")
    print(f"  Duration: {result['duration']:.6f} seconds")
    print(f"  Matched: {result['matched_count']}, Modified: {result['modified_count']}")
    print()

print("All simultaneous updates completed!")
print("Watch the change streams above to see the order of operations.")

try:
    # Keep watching for a bit longer
    print("\nContinuing to watch for changes... Press Ctrl+C to stop")
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopping all operations...")
