"""
This script filters all scrape files "scrapes_*.jsonl.zst" by minimum total Reddit score.
Unlike the original WebText we aggregate scores for all submissions containing a given
URL so the bar is slightly lower in some cases, but in others where a URL went negative
in one submission it will balance out.

The filtered scrapes file will have the original name and path of the scrape file with a 
".minscored" extension.

Arguments
---------
--scrape_directory (-dir)
    Directory containing the scrapes. You could use the overall work directory if you 
    want as we use glob.glob to search recursively.
"""

import argparse
import glob
import os
import sys
import math
from functools import reduce
from operator import add

import tqdm
from tqdm_multiprocess import TqdmMultiProcessPool

from utils.archiver import Reader, Archive

import logging
from utils.logger import setup_logger_tqdm
logger = logging.getLogger(__name__)

million = math.pow(10, 6)

# Multiprocessed
def process_file(file_path, tqdm_func, global_tqdm):
    reader = Reader()

    filtered_archive_path = file_path + ".minscored"
    archiver = Archive(filtered_archive_path)

    for document, metadata in reader.read_jsonl(file_path, get_meta=True):
        total_score = reduce(add, metadata["reddit_scores"])
        if total_score >= 3:
            archiver.add_data(document, metadata)

    global_tqdm.update(os.path.getsize(file_path))
    archiver.commit()

def filter_from_reddit_scores(scrape_directory):
    files = glob.glob(os.path.join(scrape_directory, "**/scrapes_*.jsonl.zst"), recursive=True)
    total_file_size = reduce(add, map(os.path.getsize, files))
    logger.info(f"Total File Size: {(total_file_size / million):.2f} MB")

    # [(file_name, [doc0_minhash, doc1_minhash, ...]), ....]
    with tqdm.tqdm(total=total_file_size, dynamic_ncols=True, unit_scale=1) as progress:
        pool = TqdmMultiProcessPool()
        process_count = 4
        tasks = []
        for file_path in files:
            task = (process_file, (file_path,))
            tasks.append(task)

        on_done = lambda _ : None
        on_error = on_done
        result = pool.map(process_count, progress, tasks, on_error, on_done)

    return result

parser_description = 'Filter scrapes based on minimum reddit scores.'
parser = argparse.ArgumentParser(description=parser_description)
parser.add_argument("-dir", "--scrape_directory", default="")

if __name__ == '__main__':
    args = parser.parse_args()
    if not os.path.isdir(args.scrape_directory):
        print("Scrape directory doesn't exist, exiting.")
        sys.exit(0)

    log_file = "filter_from_reddit_scores.log"
    setup_logger_tqdm(log_file)
    
    logger.info("Filtering scrapes based on minimum reddit scores.")
    filter_from_reddit_scores(args.scrape_directory)  
    
