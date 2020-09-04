import time
import datetime
import requests
import os
import logging
from logger import setup_logger
from dateutil.relativedelta import *
import tqdm
import math
import hashlib

logger = logging.getLogger()

million = math.pow(10, 6)

possible_archive_formats = ["zst", "xz", "bz2"]

def calculate_file_sha256sum(file_path):
    sha256_hash = hashlib.sha256()

    with open(file_path,"rb") as f:
        for byte_block in iter(lambda: f.read(4096),b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()

def download_file(url, local_directory):
    local_file_path = os.path.join(local_directory, url.split('/')[-1])
    logger.info(f"Attempting to download '{url}' to '{local_file_path}")

    file_size_req = requests.get(url, stream=True)
    file_size_req.raise_for_status()

    file_size = None
    if "Content-Length" in file_size_req.headers:
        file_size = int(file_size_req.headers['Content-length'])
        file_size_mb = int(file_size / million)
        logger.info(f"File Size: {file_size_mb}MB")
    else:
        logger.info("No content length header from server. RUUUUDE!")

    chunk_size = 8192
    with requests.get(url, stream=True) as r, \
         tqdm.tqdm(total=file_size, unit="byte", unit_scale=1) as progress:
        
        progress.set_description(f"Downloading")

        r.raise_for_status()
        with open(local_file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size): 
                progress.update(len(chunk))
                f.write(chunk)

def download_pushshift_dumps(start_date, end_date, download_directory):
    if not os.path.isdir(download_directory):
        os.makedirs(download_directory, exist_ok=True)

    # First available file: RS_2011-01.bz2
    base_url = "https://files.pushshift.io/reddit/submissions/"

    date = start_date
    while date <= end_date:
        for extension in possible_archive_formats:
            year = date.strftime("%Y")
            month = date.strftime("%m")
            file_name = f"RS_{year}-{month}.{extension}"
            url = f"{base_url}{file_name}"
            try:
                download_file(url, download_directory)
                logger.info("Succeeded.")                
                break # Particular month done, next
            except Exception as ex:
                logger.info(f"Failed: {ex}\n")
                pass
            time.sleep(1) # I got blocked spamming

        date = date + relativedelta(months=+1)

def get_sha256sums(download_directory):
    sha256sum_url = "https://files.pushshift.io/reddit/submissions/sha256sums.txt"
    download_file(sha256sum_url, download_directory)
    local_path = os.path.join(download_directory, "sha256sums.txt")

    sha256sum_lookup = {}
    with open(local_path, "r") as fh:
        for line in fh:
            if line.strip():
                sha256sum, file_name = tuple(line.strip().split("  "))
                sha256sum_lookup[file_name] = sha256sum

    return sha256sum_lookup


# We do this at the end to make code cleaner and allow separate verification
def verify_dumps(start_date, end_date, download_directory):

    sha256sum_lookup = get_sha256sums(download_directory)
    missing_months = []
    missing_hashes = []
    bad_hashes = []

    date = start_date
    while date <= end_date:
        found = False
        for extension in possible_archive_formats:
            year = date.strftime("%Y")
            month = date.strftime("%m")
            file_name = f"RS_{year}-{month}.{extension}"
            file_path = os.path.join(download_directory, file_name)
            if os.path.exists(file_path):
                found = True

                if file_name not in sha256sum_lookup:
                    logger.info(f"No sha256 found for {file_path}")
                    missing_hashes.append(path)
                else:  
                    calculated_hash = calculate_file_sha256sum(file_path)
     
                    if calculated_hash != sha256sum_lookup[file_name]:
                        logger.info(f"sha256 doesn't match for file {file_path}")                        
                        bad_hashes.append(file_name)
                    else:
                        logger.info(f"{file_path} hash validated")
                
                break

        if not found:
            missing_months.append(date.strftime("%Y-%m"))

        date = date + relativedelta(months=+1)

    if missing_months:
        logger.info("Missing dumps:")
        for missed in missing_months:
            logger.info(missed)
    if missing_hashes:
        logger.info("Files missing hashes:")
        for missed in missing_hashes:
            logger.info(missed)
    if bad_hashes:
        logger.info("Files with invalid hashes:")
        for bad in bad_hashes:
            logger.info(bad)

def main(logfile_path, start_date, end_date, download_directory):
    setup_logger(logfile_path)
    logger.info("Yess me loord.")

    download_pushshift_dumps(start_date, end_date, download_directory)
    logger.info("Download step complete")

    logger.info("Verifying months... ")
    verify_dumps(start_date, end_date, download_directory)

    logger.info("Job done.")

if __name__ == '__main__':
    logfile_path = "download_pushshift_dumps.log"

    # start_date = datetime.datetime(2011, 1, 1)    
    # end_date = datetime.datetime.now()

    # start_date = datetime.datetime(2020, 4, 1)    
    # end_date = datetime.datetime.now()

    start_date = datetime.datetime(2011, 1, 1)
    end_date = datetime.datetime(2011, 1, 1)

    download_directory = "E:/Eleuther_AI/webtext2/dumps"

    main(logfile_path, start_date, end_date, download_directory)