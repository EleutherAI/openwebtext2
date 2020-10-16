"""
This module is responsible for downloading the PushShift submission dump
files, and contains the following functions:

Functions
---------
calculate_file_sha256sum(file_path)
    Calculates the sha256sum of the file located at 'file_path'

get_url_content_length(url)
    Attempts to retrieve the Content-Length header when performing
    a get request on the provided 'url'. Returns the number of bytes
    if available, or None.

verify_file(file_path, expected_size, sha256sum)
    Firstly attempts to verify the file located at 'file_path' against
    the provided 'sha256sum'. If this is not available then a file size
    check is performed. Returns False if a bad hash is provided or 
    the file doesn't match expected file size, otherwise returns True.

download_file(url, local_file_path, sha256sum, tqdm_func, overall_progress=None)
    Downloades the file from 'url' to 'local_file_path', verifying against
    the provided 'sha256sum' or file size otherwise. Will retry up to 3 
    times if the download or verification fails. Returns True on success
    or False otherwise. Supports tqdm-multiprocess.

build_file_list(start_date, end_date):
    Builds a list of PushShift submission dump files located at 
    "https://files.pushshift.io/reddit/submissions" within the desired date
    range.

get_sha256sums
    Downloads the sha256sum file for the PushShift submission dumps from
    "https://files.pushshift.io/reddit/submissions/sha256sums.txt". Builds
    and returns a dictionary with file name as key and sha256 sum as value.
"""

import os
from dateutil.relativedelta import *
import math
import hashlib

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import logging
logger = logging.getLogger(__name__)

million = math.pow(10, 6)
possible_archive_formats = ["zst", "xz", "bz2"]

def calculate_file_sha256sum(file_path):
    sha256_hash = hashlib.sha256()

    with open(file_path,"rb") as f:
        for byte_block in iter(lambda: f.read(4096),b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()

def get_url_content_length(url):
    response = requests.head(url)
    response.raise_for_status()

    if "Content-Length" in response.headers:
        return int(response.headers['Content-length'])
    else:
        return None

retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)

def verify_file(file_path, expected_size, sha256sum):
    if sha256sum:
        calculated_hash = calculate_file_sha256sum(file_path)     
        if calculated_hash != sha256sum:
            logger.info("Bad hash, validation failed")
            return False
    else:
        logger.info("No hash available, testing file size.")
        file_size = os.path.getsize(file_path)
        logger.info(f"File Size: {file_size}")
        if expected_size != file_size:
            logger.info("File sizes don't match, validation failed.")
            return False

    return True

def download_file(url, local_file_path, sha256sum, tqdm_func, overall_progress=None):
    logger.info(f"Attempting to download '{url}' to '{local_file_path}")
    file_size = get_url_content_length(url)
    logger.info(f"Expected File Size: {file_size}")

    max_retries = 3
    fail_count = 0
    while True:
        if os.path.exists(local_file_path):
            logger.info("Verifying...")
            if verify_file(local_file_path, file_size, sha256sum):
                return True
            else:
                fail_count += 1

        chunk_size = 8192
        with tqdm_func(total=file_size, unit="byte", unit_scale=1) as progress:
            try:
                with session.get(url, stream=True) as r, \
                     open(local_file_path, 'wb') as f:
                    r.raise_for_status()
                    for chunk in r.iter_content(chunk_size): 
                        if overall_progress:
                            overall_progress.update(len(chunk))
                        progress.update(len(chunk))
                        f.write(chunk)
            except Exception as ex:
                logger.info(f"Download error: {ex}")
                fail_count += 1
            
        if fail_count == max_retries:
            return False

    return True

def build_file_list(start_date, end_date):
    base_url = "https://files.pushshift.io/reddit/submissions"
    url_list = []
    date = start_date
    while date <= end_date:
        year = date.strftime("%Y")
        month = date.strftime("%m")

        if year < "2011":
            url = f"{base_url}/RS_v2_{year}-{month}.xz"
            url_list.append(url)
        else:
            for extension in possible_archive_formats:
                url = f"{base_url}/RS_{year}-{month}.{extension}"
                try:
                    get_url_content_length(url) # If this fails there's no file
                    url_list.append(url)
                    break
                except:
                    pass

        date = date + relativedelta(months=+1)

    return url_list

def get_sha256sums():
    sha256sum_url = "https://files.pushshift.io/reddit/submissions/sha256sums.txt"

    sha256sum_lookup = {}
    with session.get(sha256sum_url) as response:
        response.raise_for_status()
        for line in response.text.splitlines():
            if line.strip():
                sha256sum, file_name = tuple(line.strip().split("  "))
                sha256sum_lookup[file_name] = sha256sum

    return sha256sum_lookup