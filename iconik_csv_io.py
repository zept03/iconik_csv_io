import json 
import requests
import csv
import configargparse
import os
import getpass
import datetime as dt 
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from tqdm import tqdm

# Define the CSV file path variable
csv_file_path = '/Users/sept/Documents/Designs/Clients/OpenStore/Python Scripts/Iconik Metadata/iconik_csv_io/CSV Outputs/Collections List/collections_hierarchy_test.csv'

# Function to handle retrying failed requests
def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# Parse command-line arguments
parser = configargparse.ArgParser(default_config_files=['config.ini'],description='Import/Export metadata using CSV Files')
parser.add('-c','--config_file',is_config_file=True,help="Path to custom config file")
parser.add('-m','--mode',type=str,help="Must be either 'collection' or 'saved_search' or 'search'")
parser.add('-s','--search_terms',type=str,help="Search string")
parser.add('-v','--metadata_view',type=str,help="UUID of metadata view you want to work with",required=True)
parser.add('-a','--app_id',type=str,help="iconik App-ID")
parser.add('-t','--auth_token',type=str,help="iconik Auth Token")
parser.add('-i','--input_file',type=str,help="Properly formatted importable CSV file")
parser.add('-o','--output_dir',type=str,help="Destination for exported CSV directory")
parser.add('-cp','--csv_path',type=str,help="Path to the CSV file containing collection IDs")
cli_args = parser.parse_args()

# Validate inputs
if cli_args.input_file is not None and cli_args.output_dir is not None:
    print("You've set both an input and export file, please only choose one")
    exit()

if cli_args.input_file is None and cli_args.output_dir is None:
    print("You've set no inputs or outputs, please choose one")
    exit()

if cli_args.input_file is not None:
    if not os.path.isfile(cli_args.input_file):
        print('Input file could not be found, exiting.')
        exit()
    else:
        job_mode = "Input"

if cli_args.output_dir is not None:
    if not os.path.isdir(cli_args.output_dir):
        print('Output file directory could not be found, exiting.')
        exit()
    elif cli_args.mode is None or cli_args.search_terms is None:
        print('Attempting to output but no mode or search string specified')
        exit()
    else:
        job_mode = "Output"

if cli_args.mode is not None:
    if cli_args.mode == "search":
        if cli_args.search_terms is None:
            print('You have to provide search terms if your mode is "search"')

if cli_args.app_id is None or cli_args.auth_token is None:
    auth_method = "simple"
else:
    auth_method = "api"

# Get authentication credentials
if auth_method == "simple":
    print("No App ID or Token specified in CLI or config file, assuming standard auth")
    username = input("iconik username: ")
    password = getpass.getpass("iconik password: ")
    r = requests.post('https://app.iconik.io/API/auth/v1/auth/simple/login/',headers={'accept':'application/json','content-type':'application/json'},data=json.dumps({'app_name':'WEB','email':username,'password':password}))
    if r.status_code == 201:
        app_id = r.json()['app_id']
        auth_token = r.json()['token']
    else:
        print('Auth failed - status code ' + str(r.status_code))
        for error in r.json()['errors']:
            print(error)
        exit()
else:
    app_id = cli_args.app_id
    auth_token = cli_args.auth_token

# Set global headers and URL
headers = {'App-ID':app_id,'Auth-Token':auth_token,'accept':'application/json','content-type':'application/json'}
iconik_url = 'https://app.iconik.io/API/'

# Function to get column list from a metadata view for the CSV file
def get_csv_columns_from_view(metadata_view):
    r = requests_retry_session().get(iconik_url + 'metadata/v1/views/' + metadata_view, headers=headers)
    if r.status_code == 200:
        csv_columns = []
        for field in r.json()['view_fields']:
            if field['name'] != "__separator__":
                csv_columns.append(field['name'])
        return csv_columns
    else:
        print("Error Fetching Metadata View ID " + metadata_view)
        for error in r.json()['errors']:
            print(error)
        exit()

# Function to get all results in a saved search, return the full object list with metadata
def get_saved_search_assets(saved_search_id):
    r = requests_retry_session().get(iconik_url + 'search/v1/search/saved/' + saved_search_id, headers=headers)
    if r.status_code == 200:
        search_doc = r.json()['search_criteria_document']['criteria']
    else:
        print("Error Fetching Saved Search ID " + saved_search_id)
        for error in r.json()['errors']:
            print(error)
        exit()

    search_doc['metadata_view_id'] = cli_args.metadata_view

    r = requests_retry_session().post(iconik_url + 'search/v1/search/', headers=headers, data=json.dumps(search_doc), params={'per_page': '150', 'scroll': 'true', 'generate_signed_url': 'false', 'save_search_history': 'false'})
    if r.status_code == 200:
        results = r.json()['objects']
        while len(r.json()['objects']) > 0:
            r = requests_retry_session().post(iconik_url + 'search/v1/search', headers=headers, params={'scroll': 'true', 'scroll_id': r.json()['scroll_id']}, data=json.dumps(search_doc))
            results = results + r.json()['objects']
        return results

# Function to get all results from a search query and return the full object list with metadata
def get_search_assets(search_terms):
    search_doc = {"doc_types": ["assets"], "query": search_terms, "metadata_view_id": cli_args.metadata_view}
    r = requests_retry_session().post(iconik_url + 'search/v1/search/', headers=headers, data=json.dumps(search_doc), params={'per_page': '150', 'scroll': 'true', 'generate_signed_url': 'false', 'save_search_history': 'false'})
    if r.status_code == 200:
        results = r.json()['objects']
        while len(r.json()['objects']) > 0:
            r = requests_retry_session().post(iconik_url + 'search/v1/search', headers=headers, params={'scroll': 'true', 'scroll_id': r.json()['scroll_id']}, data=json.dumps(search_doc))
            results = results + r.json()['objects']
        return results
    else:
        print("Error Running Search with terms " + cli_args.search_terms)
        for error in r.json()['errors']:
            print(error)
        exit()

# Function to get all results from a collection and return the full object list with metadata
def get_collection_assets(collection_id):
    search_doc = {"doc_types": ["assets"], "query": "", "metadata_view_id": cli_args.metadata_view,"filter":{"operator":"AND","terms":[{"name":"in_collections","value":collection_id}]}}
    r = requests_retry_session().post(iconik_url + 'search/v1/search/', headers=headers, data=json.dumps(search_doc), params={'per_page': '150', 'scroll': 'true', 'generate_signed_url': 'false', 'save_search_history': 'false'})
    if r.status_code == 200:
        results = r.json()['objects']
        while len(r.json()['objects']) > 0:
            r = requests_retry_session().post(iconik_url + 'search/v1/search', headers=headers, params={'scroll': 'true', 'scroll_id': r.json()['scroll_id']}, data=json.dumps(search_doc))
            results = results + r.json()['objects']
        return results
    else:
        print("Error listing collection with id " + cli_args.search_terms)
        for error in r.json()['errors']:
            print(error)
        exit()

# Function to update iconik metadata
def update_metadata(asset_id, metadata_doc):
    r = requests_retry_session().put(f"{iconik_url}/metadata/v1/assets/{asset_id}/views/{cli_args.metadata_view}/", data=json.dumps(metadata_doc), headers=headers)
    if r.status_code == 200:
        return True
    else:
        return False

# Function to update iconik asset title
def update_title(asset_id, title_doc):
    r = requests_retry_session().patch(f"{iconik_url}/assets/v1/assets/{asset_id}/", data=json.dumps(title_doc), headers=headers)
    if r.status_code == 200:
        return True
    else:
        return False

# Function to build a CSV file
def build_csv_file(iconik_results, metadata_field_list):
    today = dt.datetime.now().strftime("%m-%d-%Y %Hh%Mm%Ss")
    filename = f"{cli_args.search_terms} - {today}.csv"
    with open(cli_args.output_dir + '/' + filename,'w',newline='') as csvfile:
        metadata_file = csv.writer(csvfile,delimiter=',',quotechar='"')
        metadata_file.writerow(['id','title'] + metadata_field_list)
        columns = len(metadata_field_list)
        for asset in iconik_results:
            row = []
            row.append(asset['id'])
            row.append(asset['title'])
            for _ in range(columns):
                try:
                    if isinstance(asset['metadata'][metadata_field_list[_]], list):
                        row.append(','.join(map(str,asset['metadata'][metadata_field_list[_]])))
                    else:
                        row.append(asset['metadata'][metadata_field_list[_]])
                except:
                    row.append("")
            metadata_file.writerow(row)
        print(f"File successfully saved to {cli_args.output_dir}/{filename}")
        return True

# Function to read a CSV file, turn into iconik metadata, and update iconik
def read_csv_file(input_file):
    with open(input_file, newline='') as csvfile:
        metadata_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        fields = next(metadata_reader)  # Skip header row
        num_assets_updated = 0  # Initialize a counter for updated assets

        # Get total number of rows for progress bar
        total_rows = sum(1 for row in metadata_reader)

        # Reset file pointer to beginning
        csvfile.seek(0)
        next(metadata_reader)  # Skip header row

        # Wrap the reader with tqdm for a progress bar
        progress_bar = tqdm(metadata_reader, total=total_rows, desc="Updating metadata", unit="asset")

        for row in progress_bar:
            asset_id = row[0]
            this_title = {'title': row[1]}
            collection_id = row[2]  # Assuming the collection ID is in the third column
            this_metadata = {'metadata_values': {}}
            for count, value in enumerate(row[3:]):
                field_name = fields[count + 3]  # Adjust index to account for skipped columns
                this_metadata['metadata_values'][field_name] = {'field_values': []}
                field_values = value.split(",")
                for field_value in field_values:
                    if field_value.strip():  # Check if the value is not empty
                        this_metadata['metadata_values'][field_name]['field_values'].append({'value': field_value.strip()})

            # Now we update both title and metadata, and increment the progress bar
            if update_title(asset_id, this_title) and update_metadata(asset_id, this_metadata):
                num_assets_updated += 1  # Increment counter if update is successful
                progress_bar.set_description(f"Updated {num_assets_updated}/{total_rows} assets")

        print(f"Successfully updated {num_assets_updated} assets")
        return True

# Main logic
if job_mode == "Input":
    read_csv_file(cli_args.input_file)
elif job_mode == "Output":
    if cli_args.mode == 'saved_search':
        assets = get_saved_search_assets(cli_args.search_terms)
    elif cli_args.mode == 'collection':
        if cli_args.csv_path is not None:
            with open(cli_args.csv_path, newline='') as csvpathfile:
                csv_reader = csv.reader(csvpathfile)
                for row in csv_reader:
                    collection_id = row[0]
                    assets = get_collection_assets(collection_id)
                    if build_csv_file(assets, get_csv_columns_from_view(cli_args.metadata_view)):
                        print(f"CSV file for collection {collection_id} created successfully")
                    else:
                        print(f"Error creating CSV file for collection {collection_id}")
                    time.sleep(1)  # Add a small delay to prevent overwhelming the server
        else:
            print("CSV path for collection IDs not provided.")
    elif cli_args.mode == 'search':
        assets = get_search_assets(cli_args.search_terms)
    else:
        print(f"I don't know what {cli_args.mode} means.  Exiting.")
        exit()
    if build_csv_file(assets, get_csv_columns_from_view(cli_args.metadata_view)):
        print(f"Script finished successfully")
        exit()
    else:
        print(f"We ran into an error somewhere")
else:
    print(f"You have managed to divide by zero and create a singularity - run!")
    exit()
