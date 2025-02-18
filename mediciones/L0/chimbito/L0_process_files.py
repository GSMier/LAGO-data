import os
import bz2
import hashlib
import json

def sha256_hash(file_path):
    """Calculate the SHA-256 hash of a file."""
    hasher = hashlib.sha256()
    try:
        with bz2.open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
    except OSError as e:
        print(f"Error reading {file_path}: {e}")
        return None
    return hasher.hexdigest()

def parse_mtd_file(mtd_file_path):
    """Parse the .mtd file to extract metadata."""
    metadata = {}
    try:
        with bz2.open(mtd_file_path, 'rt') as f:
            for line in f:
                key, value = line.split('=', 1)
                metadata[key.strip()] = value.strip()
    except OSError as e:
        print(f"Error reading {mtd_file_path}: {e}")
        return None
    return metadata

def process_files(input_folder, output_folder):
    """Process .dat.bz2 and .mtd.bz2 files to generate JSON."""
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Group files by their common timestamp prefix
    file_groups = {}
    for file_name in os.listdir(input_folder):
        if file_name.endswith('.dat.bz2') or file_name.endswith('.mtd.bz2'):
            base_name = file_name.rsplit('.', 2)[0]  # Remove extensions
            file_groups.setdefault(base_name, []).append(file_name)

    # Process each file group
    for base_name, files in file_groups.items():
        if len(files) != 2:
            print(f"Skipping incomplete pair for: {base_name}")
            continue

        dat_file = os.path.join(input_folder, f"{base_name}.dat.bz2")
        mtd_file = os.path.join(input_folder, f"{base_name}.mtd.bz2")
        output_file = os.path.join(output_folder, f"{base_name}.json")

        # Calculate SHA-256 hashes
        dat_hash = sha256_hash(dat_file)
        mtd_hash = sha256_hash(mtd_file)

        if dat_hash is None or mtd_hash is None:
            print(f"Skipping file group {base_name} due to read error.")
            continue

        # Parse the .mtd file to extract metadata
        metadata = parse_mtd_file(mtd_file)
        if metadata is None:
            print(f"Skipping file group {base_name} due to metadata read error.")
            continue
        root_path = metadata.get("detector1Name", None).replace('"', "")

        # Extract metadata from the file name
        _, site_name, date, time = base_name.split('_', 3)
        generation_date = f"{date} {time.replace('h', ':')}:00"

        # Create JSON structure
        json_data = {
            "Id": base_name,
            "type": "L0",
            "generationDate": generation_date,
            "metadata": {
                "hash": mtd_hash,
                "location": f"/{root_path}/{base_name}.mtd.bz2"
            },
            "rawData": {
                "hash": dat_hash,
                "location": f"/{root_path}/{base_name}.dat.bz2"
            },
            "siteName": metadata.get("siteInst", None).replace('"', ""),
            "collaboratorName": metadata.get("siteRespName", None).replace('"', ""),
            "orcid": metadata.get("siteRespId", None).replace('"', ""),
        }

        # Write JSON to output file
        with open(output_file, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
        print(f"Generated JSON: {output_file}")

if __name__ == "__main__":
    input_folder = "./input"  # Input folder containing the files
    output_folder = "./output"  # Folder to save the generated JSON files
    process_files(input_folder, output_folder)

