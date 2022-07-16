"""
Builds a list of PushShift submission dump files located in "https://files.pushshift.io/reddit/submissions"
within the desired date range, and then performs the following steps for each file. Note this can't be done 
with a multiprocessing pool due to locking issues with sqlite.

1. Download and verify the file using the available sha256 sums
2. Process the file, storing the url and relevant Reddit metadata into the sqlite database
   specified in alembic.ini (copy alembic.ini.template and set sqlalchemy.url).
3. Create a .dbdone file to mark the particular file as being processed, allowing script resume.
4. Delete the PushShift dump file to save storage space if --keep_dumps not specified.

Arguments
---------
--start_period (-s)
    Month and Year of first pushshift dump. Default: 6,2005
--finish_period (-f)
    Month and Year of final pushshift dump. Defaults to current month, ignoring any missing months.
--output_directory (-dir)
    Base directory that will contain the dumps subdirectory created as part of the process.
--keep_dumps (-kd)
    If specified the dump won't be deleted after successful processing.    
"""

import datetime
import os
import argparse
import sys

from best_download import download_file
import cutie
import tqdm

from .download_pushshift_dumps import build_file_list, get_sha256sums
from .process_dump_files_sqlite import process_dump_file
from .models import get_db_session

import logging
from utils.logger import setup_logger_tqdm
logger = logging.getLogger(__name__)

def reddit_processing(url, sha256sums, dumps_directory, keep_dumps):

    base_name = url.split('/')[-1]
    dump_file_path = os.path.join(dumps_directory, base_name)
    db_done_file = dump_file_path + ".dbdone"

    if os.path.exists(db_done_file):
        return True

    try:
        download_file(url, sha256sums.get(base_name), dump_file_path)
    except Exception as ex:
        logger.info(f"Download failed {ex}, skipping processing.")
        return False

    db_session = get_db_session()
    process_dump_file(dump_file_path, db_session, tqdm.tqdm)

    with open(db_done_file, "w") as fh:
        fh.write("Done!")

    if not keep_dumps:
        os.remove(dump_file_path)

    return True

parser = argparse.ArgumentParser(description='Download PushShift submission dumps, extra urls')
parser.add_argument("-s", "--start_period", default="6,2005")
parser.add_argument("-f", "--finish_period", default=None)
parser.add_argument("-dir", "--output_directory", default="")
parser.add_argument("-kd", "--keep_dumps", action='store_true')

# First available file: https://files.pushshift.io/reddit/submissions/RS_v2_2005-06.xz
def main():
    logfile_path = "download_pushshift_dumps.log"
    setup_logger_tqdm(logfile_path) # Logger will write messages using tqdm.write

    args = parser.parse_args()

    start_month, start_year = tuple(map(int,args.start_period.split(",")))
    start_date = datetime.datetime(start_year, start_month, 1) 

    if args.finish_period:
        finish_month, finish_year = tuple(map(int,args.finish_period.split(",")))
        end_date = datetime.datetime(finish_year, finish_month, 1) 
    else:
        end_date = datetime.datetime.now()

    logger.info("Running Script - PushShift submission dumps to sqlite")
    logger.info("Downloading and processing dumps in the following range:")
    logger.info(start_date.strftime("Start Period: %m-%Y"))
    logger.info(end_date.strftime("End Period: %m-%Y"))    

    dumps_directory = os.path.join(args.output_directory, "dumps")

    if os.path.isdir(dumps_directory):
        message = f"Directory '{dumps_directory}' already exists, if there are done files" \
                   " in the directory then these particular months will be skipped. Delete" \
                   " these files or the directory to avoid this."
        logger.info(message)
        if not cutie.prompt_yes_or_no('Do you want to continue?'):
            sys.exit(0)

    os.makedirs(dumps_directory, exist_ok=True)

    logger.info("Building PushShift submission dump file list...")
    url_list = build_file_list(start_date, end_date)        

    logger.info("Getting sha256sums")
    sha256sums = get_sha256sums()

    # Download and Process
    logger.info("Commencing download and processing into sqlite.")
    results = []
    for url in url_list:
        result = reddit_processing(url, sha256sums, dumps_directory, args.keep_dumps)
        results.append(result)

if __name__ == '__main__':    
    main()  

