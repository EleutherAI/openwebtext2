import datetime
import os
import argparse
import sys
import subprocess
import csv
import json

from .download_pushshift_dumps import build_file_list, get_sha256sums, download_file
from utils.archiver import Archive

import cutie
import tqdm

import logging
from utils.logger import setup_logger_tqdm
logger = logging.getLogger(__name__)

def build_tsv(url, sha256sums, output_directory, dumps_directory, keep_dumps):

    base_name = url.split('/')[-1]
    dump_file_path = os.path.join(dumps_directory, base_name)
    db_done_file = dump_file_path + ".dbdone"

    if os.path.exists(db_done_file):
        return True

    result = download_file(url, dump_file_path, sha256sums.get(base_name), tqdm.tqdm)
    if not result:
        logger.info("Download failed, skipping processing.")
        return False

    temp_json = os.path.join(output_directory, "urls.dat")

    extension = dump_file_path.split(".")[-1]

    logger.info("Processing Dump File...")

    if extension == "zst":
        command = f'zstdcat {dump_file_path} | jq -c -r "[.url, .id, .subreddit, .title, .score, .created_utc] | @tsv" >> {temp_json}'
    elif extension == "bz2":
        command = f'bzcat {dump_file_path} | jq -c -r "[.url, .id, .subreddit, .title, .score, .created_utc] | @tsv" >> {temp_json}'
    elif extension == "xz":
        command = f'xzcat {dump_file_path} | jq -c -r "[.url, .id, .subreddit, .title, .score, .created_utc] | @tsv" >> {temp_json}'
    
    logger.info(command)
    subprocess.call(command, shell=True)

    with open(db_done_file, "w") as fh:
        fh.write("Done!")

    if not keep_dumps:
        os.remove(dump_file_path)

    return True

def generate_urls(sorted_urls_file, urls_directory, urls_per_file):

    url_batch = 0
    url_file_path = os.path.join(urls_directory, f"urls_{url_batch}.jsonl.zst")
    archiver = Archive(url_file_path)
    current_url = ""
    current_meta = {}

    total_url_count = 0
    url_count = 0
    with open(sorted_urls_file, "r") as fh:
        reader = csv.reader(fh, delimiter='\t')
        for row in reader:
            url, submission_id, subreddit, title, score, created_utc = tuple(row)

            if url != current_url:
                if current_url:
                    archiver.add_data(current_url, current_meta)

                url_count += 1
                total_url_count += 1
                current_url = url
                current_meta = {}
                current_meta["id"] = []
                current_meta["score"] = []
                current_meta["title"] = []
                current_meta["subreddit"] = []
                current_meta["created_utc"] = []

                if url_count == urls_per_file:
                    archiver.commit()
                    url_batch += 1
                    url_file_path = os.path.join(urls_directory, f"urls_{url_batch}.jsonl.zst")
                    archiver = Archive(url_file_path)
                    url_count = 0

            current_meta["id"].append(submission_id)
            current_meta["score"].append(score)
            current_meta["title"].append(title)
            current_meta["subreddit"].append(subreddit)
            current_meta["created_utc"].append(created_utc)

    if url_count > 0:
        archiver.add_data(current_url, current_meta)
        total_url_count += 1        
        archiver.commit()

    url_count_path = os.path.join(urls_directory, "url_count.json")
    json.dump(total_url_count, open(url_count_path, "w"))

parser = argparse.ArgumentParser(description='Download PushShift submission dumps, extra urls')
parser.add_argument("-s", "--start_period", default="6,2005")
parser.add_argument("-f", "--finish_period", default=None)
parser.add_argument("-dir", "--output_directory", default="")
parser.add_argument("-kd", "--keep_dumps", action='store_true')

# First available file: https://files.pushshift.io/reddit/submissions/RS_v2_2005-06.xz
def main():
    logfile_path = "pushshift_to_file.log"
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
        result = build_tsv(url, sha256sums, args.output_directory, dumps_directory, 
            args.keep_dumps)

        results.append(result)

    temp_json = os.path.join(args.output_directory, "urls.dat")
    sorted_json = os.path.join(args.output_directory, "urls_sorted.dat")

    logger.info("Sorting URLs, this will take quite a while...")
    command = f'cat {temp_json} | sort -k 1 > {sorted_json}'
    logger.info(command)    
    subprocess.call(command, shell=True)
    os.remove(temp_json)
    logger.info("URL Sort Complete")

if __name__ == '__main__':    
    main()  