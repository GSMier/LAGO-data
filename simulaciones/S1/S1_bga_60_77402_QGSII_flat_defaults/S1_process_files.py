import os
import hashlib
import json
import bz2
from glob import glob

def sha256_hash(file_path):
    """Calculate the SHA-256 hash of a file."""
    hasher = hashlib.sha256()
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()

def sha256_bzip2_hash(file_path):
    """Calculate the SHA-256 hash of a bzip2 compressed file."""
    hasher = hashlib.sha256()
    try:
        with bz2.open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
    except OSError as e:
        print(f"Error reading {file_path}: {e}")
        return None
    return hasher.hexdigest()

def process_jsonld(file_path):
    """Extract orcid and accessUrl from a JSON-LD file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    orcid = data.get('creator', {}).get('@id')
    access_url = next((item for item in data.get('@graph', [{}]) if item.get("accessURL") != None), None).get("accessURL", None)
    servesDataset = next((item for item in data.get('@graph', [{}])  if item.get("servesDataset") != None), None).get("servesDataset",None)
    generation_date = next((item for item in data.get('@graph', [{}])  if item.get('prov:endedAtTime')!= None), None).get("prov:endedAtTime",None)
    file_id = data.get('title')
    site_name = data['title'].split('_')[1]
    type_data = data['title'].split('_')[0]

    return servesDataset, file_id, orcid, access_url, site_name, type_data, generation_date

def process_group(primary_raw, secondary_raw, primary_metadata, secondary_metadata, output_folder):
    """Process a group of files and generate JSON output based on the schema."""
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Calculate hashes
    primary_raw_hash = sha256_bzip2_hash(primary_raw)
    secondary_raw_hash = sha256_bzip2_hash(secondary_raw)
    primary_metadata_hash = sha256_hash(primary_metadata)
    secondary_metadata_hash = sha256_hash(secondary_metadata)

    # Extract orcid and accessUrl
    primary_raw_location ,file_id, orcid, access_url, site_name, type_data, generation_date = process_jsonld(primary_metadata)
    root_path = primary_raw_location.split('/')[1]
    secondary_raw_location = process_jsonld(secondary_metadata)

    # Generate JSON data
    json_data = {
        "Id": file_id.replace('.pri.bz2',""),
        "type": type_data,
        "generationDate": generation_date,
        "metadata": {
            "primary": {
                "hash": primary_metadata_hash,
                "location": f"/{root_path}/{primary_metadata}"
            },
            "secondary": {
                "hash": secondary_metadata_hash,
                "location": f"/{root_path}/{secondary_metadata}"
            }
        },
        "rawData": {
            "primary": {
                "hash": primary_raw_hash,
                "location": primary_raw_location
            },
            "secondary": {
                "hash": secondary_raw_hash,
                "location": secondary_raw_location[0]
            }
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

def find_groups(input_folder, metadata_folder, output_folder):
    """Find and process groups of files."""
    raw_files = glob(os.path.join(input_folder, "*.pri.bz2"))
    for primary_raw in raw_files:
        base_name = os.path.basename(primary_raw).replace(".pri.bz2", "")
        secondary_raw = os.path.join(input_folder, f"{base_name}.sec.bz2")
        primary_metadata = os.path.join(metadata_folder, f".{base_name}.pri.bz2.jsonld")
        secondary_metadata = os.path.join(metadata_folder, f".{base_name}.sec.bz2.jsonld")

        # Ensure all files in the group exist
        if all(os.path.exists(f) for f in [primary_raw, secondary_raw, primary_metadata, secondary_metadata]):
            process_group(primary_raw, secondary_raw, primary_metadata, secondary_metadata, output_folder)
        else:
            missing = [f for f in [primary_raw, secondary_raw, primary_metadata, secondary_metadata] if not os.path.exists(f)]
            print(f"Skipping group {base_name} due to missing files: {missing}")

if __name__ == "__main__":
    input_folder = "./input"  # Input folder containing the files
    output_folder = "./output"  # Folder to save the generated JSON files
    metadata_folder = ".metadata/"  # Folder containing the metadata files
    find_groups(input_folder, metadata_folder, output_folder)

