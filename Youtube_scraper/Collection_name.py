from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017')

# Specify the database
db = client['youtube']

# Define a dictionary of old names as keys and new names as values
rename_map = {
    'Channel 1': 'The Book Show',
    'Channel 2': 'Big Bang Bogan',
    'Channel 3': 'Mr. GK',
    'Channel 4': 'நவீன உழவன் - Naveena Uzhavan',
    'Channel 5': 'techTFQ',
    'Channel 6': 'Why blood Same blood',
    'Channel 7': 'Madras Central',
    'Channel 8': 'Motor Vikatan',
    'Channel 9': 'Theneer Idaivelai',
}

# Iterate over each collection and rename it if the old name exists in the dictionary
for old_name, new_name in rename_map.items():
    if old_name in db.list_collection_names():
        db[old_name].rename(new_name)

# Close the MongoDB connection
client.close()

# Output a message indicating the process is complete
print("All specified collections have been renamed.")
