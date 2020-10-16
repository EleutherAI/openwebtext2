"""
This program iterates through URL files generated in step 2 above. For each file its hands out the URLs
to a multiprocessing pool for scraping. Once all URLs in the batch are scraped, the successful results are 
archived using a slightly modified version of [lm_dataformat](https://github.com/leogao2/lm_dataformat)
(thanks @bmk). The following metadata fields are saved in the metadata dict offered by lm_dataformat:

title: Web Page Title  
lang: Language detected by Newspaper scraper.  
url: Original URL.  
word_count: Total words outputted by Newspaper.  
elapsed: Scraping time.  
scraper: Always "newspaper".  
domain: Top level domain for the original URL.  
reddit_id: List of submission IDs containing URL - converted from base36.  
subreddit: List of subreddits for the corresponding submissions.  
reddit_score: List of reddit scores for the corresponding submissions.  
reddit_title: List of submissions titles for the corresponding submissions.  
reddit_created_utc: List of submissions created times for the corresponding submissions.

Arguments
---------
--job_directory (-dir)
    Base directory containing the urls subdirectory and location where the scrapes subdirectory
    will be created.
--process_count (-procs)
    Number of worker processes in the pool. Defaults to 60. Don't go above this on Windows.
--request_timeout (-timeout)
    Scraping timeout for each URL. Defaults to 30 seconds.
"""

import os
import sys
import glob
import json
import argparse

import tldextract
import tqdm
from tqdm_multiprocess import TqdmMultiProcessPool

from scraping.scrapers import newspaper_scraper
from utils.archiver import Reader, Archive
from utils.utils import Timer

import logging
from utils.logger import setup_logger_tqdm
logger = logging.getLogger(__name__)

# Multiprocessed
def download(url_entry, request_timeout, scraper,
             memoize, tqdm_func, global_tqdm):

    url, reddit_meta = url_entry    
    text, meta, success = scraper(url, memoize, request_timeout=request_timeout)

    if not success or text is None or text.strip() == "":
        if global_tqdm:
            global_tqdm.update()
        return (text, meta, False)

    # Add extra meta
    ext = tldextract.extract(url)
    domain = '.'.join([x for x in ext if x])
    meta["domain"] = domain
    meta["reddit_id"] = reddit_meta["id"]
    meta["subreddit"] = reddit_meta["subreddit"]
    meta["reddit_score"] = reddit_meta["score"]
    meta["reddit_title"] = reddit_meta["title"]
    meta["reddit_created_utc"] = reddit_meta["created_utc"]

    if global_tqdm:
        global_tqdm.update()
    return (text, meta, success)

def scrape_urls(urls_directory, scrapes_directory, process_count, request_timeout):

    # Get Total URL count (saved during sqlite extraction)
    url_count_path = os.path.join(urls_directory, "url_count.json")
    total_url_count = json.load(open(url_count_path, "r"))

    # overall progress bar
    progress = tqdm.tqdm(total=total_url_count, dynamic_ncols=True)
    progress.set_description("Total URLs")

    url_files = glob.glob(os.path.join(urls_directory, "urls_*.jsonl.zst"))

    for url_file_path in url_files:
        # Skip if previously done
        done_file_path = url_file_path + ".done"
        if os.path.exists(done_file_path):
            batch_url_count = json.load(open(done_file_path, "r"))
            progress.update(batch_url_count)
            logger.info(f"'{os.path.basename(url_file_path)}' already scraped, skipping.")

        logger.info(f"Scraping URLs from '{os.path.basename(url_file_path)}'.")
    
        reader = Reader()
        url_data = []
        for url, reddit_meta in reader.read_jsonl(url_file_path, get_meta=True):
            url_data.append((url, reddit_meta))

        timer = Timer().start()

        batch_progress = tqdm.tqdm(total=len(url_data), dynamic_ncols=True)
        batch_progress.set_description(f"{os.path.basename(url_file_path)}")

        # Download and Process With Pool
        pool = TqdmMultiProcessPool()
        tasks = []
        for url_entry in url_data:
            arguments = (url_entry, request_timeout, newspaper_scraper, False)
            task = (download, arguments)
            tasks.append(task)

        on_done = lambda _ : progress.update()
        on_error = lambda _ : None
        results = pool.map(process_count, batch_progress, tasks, on_error, on_done)

        logger.info("Archiving chunk with lm_dataformat...")
        # urls_*.jsonl.zst -> scrapes_*.jsonl.zst
        output_archive_name = os.path.basename(url_file_path).replace("urls", "scrapes")
        output_archive_path = os.path.join(scrapes_directory, output_archive_name)
        archiver = Archive(output_archive_path)
        batch_error_count = 0        
        for text, meta, status in results:
            if not status:
                batch_error_count += 1
            else:
                archiver.add_data(text, meta)
        archiver.commit()

        error_percentage = batch_error_count / len(url_data) * 100
        logger.info(f"Errors: {batch_error_count} / {len(url_data)} ({error_percentage:0.2f}%)")
        logger.info(f"Batch time: {timer.stop():0.2f} seconds")

        progress.update(len(url_data))
        batch_progress.close()

        json.dump(len(url_data), open(done_file_path, "w"))

    progress.close()
    logger.info("Done!")

parser_description = 'Scrape urls extracted from Reddit.'
parser = argparse.ArgumentParser(description=parser_description)
parser.add_argument("-dir", "--job_directory", default="")
parser.add_argument("-procs", "--process_count", type=int, default=60)
parser.add_argument("-timeout", "--request_timeout", type=int, default=30)

if __name__ == "__main__":
    logfile_name = "scrape_urls.log"
    setup_logger_tqdm(logfile_name)

    args = parser.parse_args()    

    urls_directory = os.path.join(args.job_directory, "urls")
    if not os.path.exists(urls_directory):
        logger.info(f"No 'urls' directory found in '{args.job_directory}', aborting")
        sys.exit(0)

    scrapes_directory = os.path.join(args.job_directory, "scrapes")
    os.makedirs(scrapes_directory, exist_ok=True)

    logger.info(f"Scrapes outputting to: '{scrapes_directory}'") 

    scrape_urls(urls_directory, scrapes_directory, args.process_count, args.request_timeout)    
