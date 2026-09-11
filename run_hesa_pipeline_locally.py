"""
Standalone script to run the HESA data preparation logic locally.
This script downloads the HESA ZIP archive, finds the kis.xml file, and compresses it to GZIP.
You can then add the gzip file to the hesa-raw-xml-ingest blob storage and kick off the ingestion using postman
"""

import urllib.request
import zipfile
import gzip
import os
import time
import fnmatch
import argparse

def download_file(path, dest):
    hdr = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Connection': 'keep-alive'
    }
    max_retries = 3
    retry_delay = 10
    for attempt in range(max_retries):
        try:
            print(f'Attempting to fetch file from {path}... Attempt #{attempt + 1}')
            if urllib.request.urlopen(path).getcode() == 200:
                req = urllib.request.Request(path, headers=hdr)
                with urllib.request.urlopen(req) as response:
                    print('Beginning file (download)...')
                    with open(dest, 'wb') as out_file:
                        while True:
                            data = response.read(1024 * 1024)  # 1MB chunks
                            if not data:
                                break
                            out_file.write(data)
                            print('Processed DOWNLOADED file complete!')
                            return True
            else:
                with open(path, 'rb') as response:
                    print('Beginning file (local)...')
                    with open(dest, 'wb') as out_file:
                        while True:
                            data = response.read(1024 * 1024)  # 1MB chunks
                            if not data:
                                break
                            out_file.write(data)
                        print('Processed local file!')
                        return True

        except Exception as e:
                print(f'Error during download: {e}')
                if attempt < max_retries - 1:
                    print(f'Retrying in {retry_delay} seconds...')
                    time.sleep(retry_delay)
                else:
                    raise

def main():
    parser = argparse.ArgumentParser(description='Prepare HESA data locally.')
    parser.add_argument('--path', default='https://unistatsdataset.hesa.ac.uk/api/UnistatsDatasetDownload',
                        help='The path to download the HESA ZIP from.')
    parser.add_argument('--output-dir', default='temp_hesa_data',
                        help='Directory to store temporary and output files.')
    
    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    zip_path = os.path.join(args.output_dir, 'hesa_data.zip')
    gz_path = os.path.join(args.output_dir, 'latest.xml.gz')



    # 1. Download
    try:
        if args.path.startswith('https://unistatsdataset.hesa.ac.uk'):
            download_file(args.path, zip_path)
        else:
            zip_path = args.path
    except Exception as e:
        print(f"Failed to download file: {e}")
        return

    # 2. Process ZIP
    print('Searching for kis*.xml in ZIP...')
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            kis_file_info = None
            
            # Primary search pattern
            for info in z.infolist():
                if fnmatch.fnmatch(info.filename, '*/on_*/kis*.xml') or \
                   fnmatch.fnmatch(info.filename, 'on_*/kis*.xml') or \
                   fnmatch.fnmatch(info.filename, 'kis*.xml'):
                    print(f'Found matching file in ZIP: {info.filename}')
                    kis_file_info = info
                    break
            
            # Fallback: search anywhere for kis*.xml
            if not kis_file_info:
                for info in z.infolist():
                    if fnmatch.fnmatch(os.path.basename(info.filename), 'kis*.xml'):
                        print(f'Found matching file (fallback): {info.filename}')
                        kis_file_info = info
                        break

            if kis_file_info:
                print(f'Extracting and compressing {kis_file_info.filename}...')
                with z.open(kis_file_info) as f_in:
                    with gzip.open(gz_path, 'wb') as f_out:
                        # Process in chunks to save memory
                        while True:
                            chunk = f_in.read(1024 * 1024)
                            if not chunk:
                                break
                            f_out.write(chunk)
                print(f'Preparation complete! Output saved to: {os.path.abspath(gz_path)}')
            else:
                print('No matching kis*.xml found in ZIP!')
                print('Files in ZIP:')
                for name in z.namelist():
                    print(f'  {name}')

    except zipfile.BadZipFile:
        print(f"Error: The downloaded file is not a valid ZIP archive.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
