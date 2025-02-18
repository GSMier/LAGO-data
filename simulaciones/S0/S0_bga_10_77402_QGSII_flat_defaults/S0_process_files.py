import os
import re
from glob import glob
import hashlib
import json
import bz2

def sha256_hash(file_path):
    """Calculate the SHA-256 hash of a file with ASCII encoding."""
    hasher = hashlib.sha256()
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            # Decode binary data to ASCII before hashing if applicable
            #ascii_data = chunk.decode('ascii', errors='ignore')
            #hasher.update(ascii_data.encode('ascii'))
             hasher.update(chunk)

    return hasher.hexdigest()


def sha256_bzip2_hash(file_path):
    """Calculate the SHA-256 hash of a file."""
    hasher = hashlib.sha256()
    try:
        with bz2.open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                # hasher.update(chunk)
                ascii_data = chunk.decode('ascii', errors='ignore')
                hasher.update(ascii_data.encode('ascii'))
    except OSError as e:
        print(f"Error reading {file_path}: {e}")
        return None
    return hasher.hexdigest()




def process_jsonld(file_path):
    """Extract orcid and accessUrl from a JSON-LD file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    orcid = data.get('creator', {}).get('@id')
    access_url = next((item for item in data.get('@graph', [{}])  if item.get("accessURL") != None), None).get("accessURL",None)
    servesDataset = next((item for item in data.get('@graph', [{}])  if item.get("servesDataset") != None), None).get("servesDataset",None)
    generation_date = next((item for item in data.get('@graph', [{}])  if item.get('prov:endedAtTime')!= None), None).get("prov:endedAtTime",None)
    file_id = data.get('title')
    site_name = data['title'].split('_')[1]
    type_data = data['title'].split('_')[0]

    return servesDataset, file_id, orcid, access_url, site_name, type_data, generation_date


def process_group(input_data, output_data, raw_data, metadata, input_metadata, output_metadata, output_folder):
    """Process a group of files and generate JSON output based on the schema."""
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)



    # Calculate hashes
    input_data_hash = sha256_hash(input_data)
    output_data_hash = sha256_bzip2_hash(output_data)
    raw_data_hash = sha256_bzip2_hash(raw_data)
    metadata_hash = sha256_hash(metadata)
    input_metadata_hash = sha256_hash(input_metadata)
    output_metadata_hash = sha256_hash(output_metadata)

    # Extract orcid and accessUrl
    raw_data_location, file_id, orcid, access_url, site_name, type_data, generation_date = process_jsonld(metadata)
    input_data_location = process_jsonld(input_metadata)
    output_data_location = process_jsonld(output_metadata)
    root_path = raw_data_location.split('/')[1]

    # Generate JSON data
    json_data = {
        "Id": file_id.replace('.bz2',""),
        "type": type_data,
        "generationDate": generation_date,  
        "metadata": {
            "hash": metadata_hash,
            "location": f"/{root_path}/{metadata}"
        },
        "rawData": {
            "hash": raw_data_hash,
            "location": raw_data_location
        },
        "inputData": {
            "hash": input_data_hash,
            "location": input_data_location[0]
        },
        "inputMetadata": {
            "hash": input_metadata_hash,
            "location": f"/{root_path}/{input_metadata}" 
        },
        "outputData": {
            "hash": output_data_hash,
            "location": output_data_location[0]
        },
        "outputMetadata": {
            "hash": output_metadata_hash,
            "location": f"/{root_path}/{output_metadata}"
        },
        "siteName": site_name,  
        "collaboratorName": None,
        "orcid": orcid,
        "accessUrl": access_url
    }

    # Save JSON output
    output_file = os.path.join(output_folder, f"{json_data['Id']}.json")
    with open(output_file, 'w') as json_file:
        json.dump(json_data, json_file, indent=4)

    print(f"Generated JSON: {output_file}")

def get_latest_metadata_file(base_name, metadata_folder, pattern):
    """Find the latest metadata file based on timestamp in the filename."""
    #matching_files = glob(os.path.join(metadata_folder, f".{base_name}{pattern}.*Z"))

    pattern = re.compile(rf"^\.{base_name}{pattern}\..*Z$")
    matching_files = [f for f in os.listdir(metadata_folder) if pattern.match(f)]
    if not matching_files:
        return None

    # Extract timestamp using regex
    timestamp_pattern = re.compile(r".*(\d{8}T\d{6}\.\d{6}Z)$")
    files_with_timestamps = [
        (f, timestamp_pattern.search(f).group(1)) for f in matching_files if timestamp_pattern.search(f)
    ]

    # Sort by timestamp (latest first) and return the latest file
    latest_file = max(files_with_timestamps, key=lambda x: x[1])[0] if files_with_timestamps else None
    return os.path.join(metadata_folder, latest_file)

def find_groups(input_folder, metadata_folder, output_folder):
    """Find and process groups of files."""
    input_files = glob(os.path.join(input_folder, "*.input"))
    for input_file in input_files:
        base_name = os.path.basename(input_file).replace(".input", "")
        base_name_split = base_name.split("-")[0]
        raw_data = os.path.join(input_folder, f"{base_name_split}.bz2")
        
        metadata = get_latest_metadata_file(base_name_split, metadata_folder, ".bz2.jsonld")
        input_metadata = get_latest_metadata_file(base_name, metadata_folder, ".input.jsonld")
        output_metadata = get_latest_metadata_file(base_name, metadata_folder, ".lst.bz2.jsonld")

        input_data = os.path.join(input_folder, f"{base_name}.input")
        output_data = os.path.join(input_folder, f"{base_name}.lst.bz2")

        #Ensure all required files exist
        required_files = [input_data, raw_data, metadata, input_metadata, output_data, output_metadata]
        if all(f and os.path.exists(f) for f in required_files):
            process_group(input_data, output_data, raw_data, metadata, input_metadata, output_metadata, output_folder)
        else:
            missing = [f for f in required_files if not f or not os.path.exists(f)]
            print(f"Skipping group {base_name} due to missing files: {missing}")

if __name__ == "__main__":
    input_folder = "./input"
    output_folder = "./output"
    metadata_folder = ".metadata/"
    find_groups(input_folder, metadata_folder, output_folder)
