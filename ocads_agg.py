import argparse
from urllib.parse import urljoin
from pathlib import Path
import pickle
import json
import re
import os
from multiprocessing import Pool
import time
import random


import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

metadata_index_url = "https://www.ncei.noaa.gov/data/oceans/ncei/ocads/ocads_metadata.json"
data_url = "https://www.ncei.noaa.gov/data/oceans/ncei/ocads/data/"

def get_filename_from_cd(cd):
    """
    Get filename from content-disposition
    """
    if not cd:
        return None
    fname = re.findall('filename=(.+)', cd)
    if len(fname) == 0:
        return None
    return fname[0]

def download_file(url, output_dir):
    """
    Download file from given url into output_dir.
    """
    r = requests.get(url, allow_redirects=True)
    if r.ok:
        filename = get_filename_from_cd(r.headers.get('content-disposition'))
        if not filename:
            if url.find('/'):
                filename = url.rsplit('/', 1)[1]
        filename = os.path.join(output_dir, filename)
        open(filename, 'wb').write(r.content)
        return filename
    else:
        return None
    
def check_file(url, output_dir):
    if url.find('/'):
        filename = url.rsplit('/', 1)[1]
        filename = os.path.join(output_dir, filename)
        return os.path.isfile(filename)
    else:
        raise Exception("No filename in URL. Whoops")

def fetch_latest_json(output_dir):
    """
    Download the latest copy of the metadata index file
    """
    return download_file(metadata_index_url, output_dir)

def load_metadata_index(output_dir):
    """
    Download the latest copy of the metadta index return the loaded index
    """
    # Grab the latest copy of the metadata index
    metadata_index_filename = fetch_latest_json(output_dir)
    if not metadata_index_filename:
        raise Exception("Error: unable to download the most recent metadata index.")
    
    # Load the json from the metadata index so we can loop through it and download all the stuff
    metadata_index = None
    with open(metadata_index_filename) as f:
        metadata_index = json.load(f)
    return metadata_index

def generate_dataset_url(metadata_index_entry):
    accession_number = metadata_index_entry['accession_number']
    return urljoin(data_url, accession_number)

def dataset_folder_exists(output_dir, metadata_index_entry):
    accession_number = metadata_index_entry['accession_number']
    dataset_path = os.path.join(output_dir, accession_number)
    return os.path.isdir(dataset_path)

def get_dataset_folder_paths(metadata_index_entry, output_dir):
    accession_number = metadata_index_entry['accession_number']
    metadata_path = os.path.join(output_dir, accession_number, "metadata")
    data_path = os.path.join(output_dir, accession_number, "data")
    return metadata_path, data_path

def create_dataset_folders(metadata_index_entry, output_dir):
    metadata_path, data_path = get_dataset_folder_paths(metadata_index_entry, output_dir)

    Path(metadata_path).mkdir(parents=True, exist_ok=True)
    Path(data_path).mkdir(parents=True, exist_ok=True)
    return metadata_path, data_path

def download_metadata(output_dir, metadata_index_entry):
    if 'lonlat_url' in metadata_index_entry:
        download_file(metadata_index_entry['lonlat_url'], output_dir)
    download_file(metadata_index_entry['xml_url_iso-19115-2'], output_dir)
    download_file(metadata_index_entry['xml_url_ocads'], output_dir)

def check_metadata(output_dir, metadata_index_entry):
    result = True
    if 'lonlat_url' in metadata_index_entry:
        result *= check_file(metadata_index_entry['lonlat_url'], output_dir)
    result *= check_file(metadata_index_entry['xml_url_iso-19115-2'], output_dir)
    result *= check_file(metadata_index_entry['xml_url_ocads'], output_dir)
    return bool(result)

def download_webdirectory_contents(output_dir, url):
    webdir_page = requests.get(url).text
    soup = BeautifulSoup(webdir_page, "html.parser")
    links = [urljoin(url + "/", node.get('href')) for node in soup.find_all('a')[5:]]
    
    files = []
    for link in links:
        if link[-1] == "/":
            # if we found a subdirectory rather than a file, traverse the subdirectory recursively
            subdir_path = os.path.join(output_dir, link.split("/")[-2])
            Path(subdir_path).mkdir(parents=True, exist_ok=True)
            files += download_webdirectory_contents(subdir_path, link)
        else:
            # otherwise do the normal file stuff
            file_path = download_file(link, output_dir)
            if file_path:
                files.append(file_path)
            else:
                raise Exception("Download failed for %s" % link)
    return files

def check_webdirectory_contents(output_dir, url):
    webdir_page = requests.get(url).text
    soup = BeautifulSoup(webdir_page, "html.parser")
    links = [urljoin(url + "/", node.get('href')) for node in soup.find_all('a')[5:]]

    result = True
    for link in links:
        if link[-1] == "/":
            # if we found a subdirectory rather than a file
            subdir_path = os.path.join(output_dir, link.split("/")[-2])
            if os.path.isdir(subdir_path):
                result *= check_webdirectory_contents(subdir_path, link)
            else:
                return False
        else:
            # Otherwise do file stuff
            result *= check_file(link, output_dir)
    return bool(result)


def load_state():
    path = Path(__file__).with_name('state.bin')
    try:
        with open(path, "rb") as f:
            return pickle.load(f)
    except FileNotFoundError:
        return []

def save_state(aggregated_datasets):
    path = Path(__file__).with_name('state.bin')
    with open(path, 'wb') as f:
        pickle.dump(aggregated_datasets, f)
        return aggregated_datasets
    
def aggregate_dataset(metadata_index_entry, output_dir):
    aggregated_datasets = load_state()
    if (not dataset_folder_exists(output_dir, metadata_index_entry) or metadata_index_entry['accession_number'] not in aggregated_datasets):
        # print("Aggregating dataset %s/%s" % (index, len(metadata_index)))
        print("Downloading data and metadata for accession_number: %s" % metadata_index_entry['accession_number'])
        metadata_path, data_path = create_dataset_folders(metadata_index_entry, output_dir)
        download_metadata(metadata_path, metadata_index_entry)
        print("metadata download complete")

        dataset_url = generate_dataset_url(metadata_index_entry)
        data_files = download_webdirectory_contents(data_path, dataset_url)
        print("Successfully downloaded %s files\n" % len(data_files))
        aggregated_datasets.append(metadata_index_entry['accession_number'])
        save_state(aggregated_datasets)

def check_dataset(metadata_index_entry, output_dir):
    if (dataset_folder_exists(output_dir, metadata_index_entry)):
        # dataset folder exists, so check the files
        result = True

        metadata_path, data_path = get_dataset_folder_paths(metadata_index_entry, output_dir)
        
        metadata_result = check_metadata(metadata_path, metadata_index_entry)
        result *= metadata_result

        dataset_url = generate_dataset_url(metadata_index_entry)
        data_result = check_webdirectory_contents(data_path, dataset_url)
        result *= data_result        
        return bool(result)
    else:
        # dataset folder doesn't exist, must not have been downloaded
        return False

class DatasetAggregator(object):
    def __init__(self, output_dir):
        self.output_dir = output_dir
    def __call__(self, metadata_index_entry):
        return aggregate_dataset(metadata_index_entry, self.output_dir)
    

def main(output_dir, num_threads):
    print("Starting OCADS data aggregation process")
    
    metadata_index = load_metadata_index(output_dir)
    # Code for single-threaded approach
    # for index, metadata_index_entry in enumerate(metadata_index):
    #     aggregate_dataset(metadata_index_entry)

    # Multiprocessing
    try:
        pool = Pool(num_threads)
        dataset_aggregator = DatasetAggregator(output_dir)
        pool.map(dataset_aggregator, metadata_index)
    finally:
        pool.close()
        pool.join()    
    return

def check_aggregation(output_dir):
    print("Fetching metadata index")
    metadata_index = load_metadata_index(output_dir)
    problematic_datasets = []
    print("Starting check")
    for metadata_index_entry in tqdm(metadata_index):
        aggregation_check_passed = check_dataset(metadata_index_entry, output_dir)
        # Wait a random amount of time, between 0 and 10 seconds so we don't make the web server angry
        time.sleep(random.randrange(10))
        if not aggregation_check_passed:
            problematic_datasets.append(metadata_index_entry)

    if len(problematic_datasets) > 0:
        print("\nProblematic datasets:")
        for dataset in problematic_datasets:
            print("\t- %s" % dataset['accession_number'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser("OCADS_agg")
    parser.add_argument(
        "-o", "--output_dir",
        help="A directory to store the aggregated OCADS data in. If no directory is provided the current working directory will be used.", 
        type=str
    )
    parser.add_argument(
        "-t", "--num_threads", 
        help="The number of threads to be used in aggregating data", 
        type=int
    )
    parser.add_argument(
        "-c", "--check",
        help="Check the local backup of the data to ensure that nothing was missed.",
        action="store_true"
    )
    args = parser.parse_args()

    output_dir = os.getcwd()
    if (args.output_dir):
        output_dir = args.output_dir

    num_threads = 1
    if (args.num_threads):
        num_threads = args.num_threads

    if (args.check):
        check_aggregation(output_dir)
    else:
        main(output_dir, num_threads)