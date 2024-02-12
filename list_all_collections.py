import requests
import csv
from tqdm import tqdm

# Set your Iconik App-ID and Auth-Token here
APP_ID = 'f6c28098-c572-11ee-b72d-fee3e700ab97'
AUTH_TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjE1OTdhMTU0LWM4ZWEtMTFlZS1hNzU3LTQ2Zjc1MmMxN2FkOSIsImV4cCI6MjAyMzEyMTgyMH0.aAP5gSwZB6csnEvURo29jF9RjTPP2gr1E7z0r1fn2as'
ICONIK_URL = 'https://app.iconik.io'


headers = {
    'App-ID': APP_ID,
    'Auth-Token': AUTH_TOKEN,
}

def fetch_collection_contents(collection_id, csv_writer, parent_name="Root Collection", progress=None):
    """
    Recursively fetch collections and sub-collections, writing their names and IDs to a CSV file,
    and updating a progress bar.
    
    :param collection_id: ID of the collection to start fetching from.
    :param csv_writer: CSV writer object to write data into the CSV file.
    :param parent_name: Name of the parent collection, used for hierarchical naming.
    :param progress: tqdm progress bar object.
    """
    url = f'{ICONIK_URL}/API/assets/v1/collections/{collection_id}/contents/'
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        contents = response.json()
        for item in tqdm(contents['objects'], desc="Fetching Collections", leave=False, unit="collection", position=0 if progress is None else progress.pos + 1):
            if item['object_type'] == 'collections':
                # Construct hierarchical collection name
                collection_name = f"{parent_name} > {item['title']}"
                # Write collection name and ID to CSV
                csv_writer.writerow([collection_name, item['id']])
                # Initialize or update progress bar
                if progress is None:
                    progress = tqdm(total=0, position=0, desc="Total Collections")
                progress.update(1)
                # Recurse into sub-collection
                fetch_collection_contents(item['id'], csv_writer, collection_name, progress)
    else:
        print(f"Failed to fetch contents for collection {collection_id}. HTTP Status Code: {response.status_code}")

# Open a new CSV file to write
with open('collections_hierarchy.csv', mode='w', newline='', encoding='utf-8') as file:
    csv_writer = csv.writer(file)
    # Write the header row
    csv_writer.writerow(['Collection Name', 'Collection ID'])
    
    # Example usage: Replace 'root_collection_id' with your actual root collection ID
    root_collection_id = '9c9685b4-c537-11ee-98cc-5628bc37ea15'
    fetch_collection_contents(root_collection_id, csv_writer)

print("The collections hierarchy has been saved to 'collections_hierarchy.csv'.")
