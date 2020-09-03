import time
import datetime
import requests
import os
import logging
from logger import setup_logger
from dateutil.relativedelta import *
import tqdm
import math

logger = logging.getLogger()

million = math.pow(10, 6)

def download_file(url, local_directory):
    local_file_path = os.path.join(local_directory, url.split('/')[-1])
    logger.info(f"Attempting to download '{url}' to '{local_file_path}")

    file_size_req = requests.get(url, stream=True)
    file_size_req.raise_for_status()
    file_size = int(file_size_req.headers['Content-length'])
    file_size_mb = int(file_size / million)
    logger.info(f"File Size: {file_size_mb}MB")

    chunk_size = 8192
    with requests.get(url, stream=True) as r, \
         tqdm.tqdm(total=file_size, unit="byte", unit_scale=1) as progress:
        
        progress.set_description(f"Downloading")

        r.raise_for_status()
        with open(local_file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size): 
                progress.update(chunk_size)
                f.write(chunk)

def download_pushshift_dumps(start_date, end_date, download_directory):
    # First available file: RS_2011-01.bz2
    base_url = "https://files.pushshift.io/reddit/submissions/"
    possible_archive_formats = ["bz2", "xz", "zst"]

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

def verify_dumps(start_date, end_date, download_directory):

    possible_archive_formats = ["bz2", "xz", "zst"]
    missing = []

    date = start_date
    while date <= end_date:
        success = False
        for extension in possible_archive_formats:
            year = date.strftime("%Y")
            month = date.strftime("%m")
            file_name = f"RS_{year}-{month}.{extension}"
            file_path = os.path.join(download_directory, file_name)
            if os.path.exists(file_path):
                success = True
                break
        if not success:
            missing.append(date.strftime("%Y-%m"))

        date = date + relativedelta(months=+1)

    if missing:
        logger.info("Missing dumps:")
        for missed in missing:
            logger.info(missed)
    else:
        logger.info("No dumps missing.")

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

    start_date = datetime.datetime(2017, 12, 1)
    end_date = datetime.datetime(2017, 12, 1)

    download_directory = "E:/Eleuther_AI/webtext2/dumps"

    main(logfile_path, start_date, end_date, download_directory)