import os
import hashlib
import json
import bz2
from glob import glob


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


# def decompress_bz2(file_path):
#     """Decompress a .bz2 file and return the decompressed content."""
#     decompressed_file_path = file_path.rstrip('.bz2')  # Remove the .bz2 extension
#     if not os.path.exists(decompressed_file_path):
#         with bz2.BZ2File(file_path, 'rb') as bz2_file, open(decompressed_file_path, 'wb') as decompressed_file:
#             for data in iter(lambda: bz2_file.read(8192), b''):
#                 decompressed_file.write(data)
#         print(f"Decompressed: {file_path} -> {decompressed_file_path}")
#     return decompressed_file_path


def process_jsonld(file_path):
    """Extract orcid and accessUrl from a JSON-LD file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    orcid = data.get('creator', {}).get('@id')
    access_url = next((item for item in data.get('@graph', [{}])  if item.get("accessURL") != None), None).get("accessURL",None)
    generation_date = next((item for item in data.get('@graph', [{}])  if item.get('prov:endedAtTime')!= None), None).get("prov:endedAtTime",None)
    file_id = data.get('title')
    site_name = data['title'].split('_')[1]
    type_data = data['title'].split('_')[0]

    return file_id, orcid, access_url, site_name, type_data, generation_date


def process_group(input_data, output_data, raw_data, metadata, input_metadata, output_metadata, output_folder):
    """Process a group of files and generate JSON output based on the schema."""
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Decrypt (decompress) .bz2 files
    # raw_data = decompress_bz2(raw_data)
    # output_data = decompress_bz2(output_data)

    # Calculate hashes
    input_data_hash = sha256_hash(input_data)
    output_data_hash = sha256_bzip2_hash(output_data)
    raw_data_hash = sha256_bzip2_hash(raw_data)
    metadata_hash = sha256_hash(metadata)
    input_metadata_hash = sha256_hash(input_metadata)
    output_metadata_hash = sha256_hash(output_metadata)

    # Extract orcid and accessUrl
    file_id, orcid, access_url, site_name, type_data, generation_date = process_jsonld(metadata)

    # Generate JSON data
    json_data = {
        "Id": file_id.replace('.bz2',""),
        "type": type_data,
        "generationDate": generation_date,  # Add logic if the generation date can be extracted from the filenames
        "metadata": metadata_hash,
        "rawData": raw_data_hash,
        "inputData": input_data_hash,
        "inputMetadata": input_metadata_hash,
        "outputData": output_data_hash,
        "outputMetadata": output_metadata_hash,
        "siteName": site_name,  # Populate if there’s site information available
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
    input_files = glob(os.path.join(input_folder, "*.input"))
    for input_file in input_files:
        base_name = os.path.basename(input_file).replace(".input", "")
        base_name_split = base_name.split("-")[0]
        raw_data = os.path.join(input_folder, f"{base_name_split}.bz2")
        metadata = os.path.join(metadata_folder, "."+f"{base_name_split}.bz2.jsonld")
        input_data = os.path.join(input_folder, f"{base_name}.input")
        input_metadata = os.path.join(metadata_folder, "."+f"{base_name}.input.jsonld")
        output_data = os.path.join(input_folder, f"{base_name}.lst.bz2")
        output_metadata = os.path.join(metadata_folder,"."+f"{base_name}.lst.bz2.jsonld")

        # Ensure all files in the group exist
        if all(os.path.exists(f) for f in [input_data, raw_data, metadata, input_metadata, output_data, output_metadata]):
            process_group(input_data, output_data, raw_data, metadata, input_metadata, output_metadata, output_folder)
        else:
            missing = [f for f in [input_data, raw_data, metadata, input_metadata, output_data, output_metadata] if not os.path.exists(f)]
            print(f"Skipping group {base_name} due to missing files: {missing}")




if __name__ == "__main__":
    input_folder = "./input"  # Input folder containing the files
    output_folder = "./output"  # Folder to save the generated JSON files
    metadata_folder = "./metadata"  # Folder containing the metadata files
    find_groups(input_folder, metadata_folder, output_folder)

