"""
This script produces files containing the urls and associated Reddit metadata
for a given period, defaulting to 1,000,000 urls per file. Using lm_dataformat, 
one record is stored for each URL, along with the metadata of all submissions for 
that particular URL. 

No separate URL based deduplication is required unless you run multiple iterations of
the script across different time periods (unimplemented but very simple).

Note that we don't filter by score at this stage as the full pipeline scrapes all urls
and leaves the filtering to be done by the user if they don't want the plug and play version.

Arguments
---------
--start_period (-s)
    Month and Year of first URLs. Defaults to None (query all URLs).
--finish_period (-f)
    Month and Year of final URLs. Defaults to None (query all URLs).
--output_directory (-dir)
    Base directory that will contain the urls subdirectory created as part of the process. 
--urls_per_file
    Maximum number of urls per file. Defaults to 100,000.
--min_score
    Minimum aggregate submissions score to include url.
--data_source
    Where to find sorted URLs: "db" or "tsv". tsv doesn't support date ranges.

If both start_period and finish_period are blank then we can use a faster query on the reddit_submission
table.
"""

import datetime
from dateutil.relativedelta import *
import argparse
import os
import json
from functools import partial
import sys

from utils.archiver import Archive
from .models import RedditSubmission, get_db_session

import logging
from utils.logger import setup_logger_tqdm
logger = logging.getLogger(__name__)

def get_from_db(start_date, end_date):
    db_session = get_db_session()

    # SELECT id, url, score, title, subreddit, created_utc
    # FROM reddit_submission
    # WHERE created_utc >= start_date and created_utc <= end_date
    # ORDER BY url
    select_fields = (RedditSubmission.id, RedditSubmission.url, RedditSubmission.score,
                     RedditSubmission.title, RedditSubmission.subreddit, RedditSubmission.created_utc)

    if start_date or end_date:
        month_end = end_date + relativedelta(months=+1)
        query = db_session.query(*select_fields) \
                          .filter(RedditSubmission.created_utc >= start_date) \
                          .filter(RedditSubmission.created_utc < month_end) \
                          .order_by(RedditSubmission.url) \
                          .yield_per(1000)
    else:
        query = db_session.query(*select_fields) \
                          .order_by(RedditSubmission.url) \
                          .yield_per(1000)

    logger.info("Querying sqlite database for submissions")
    logger.info(query)
    return query

def get_from_tsv():
    pass


def generate_urls(url_directory, urls_per_file, min_score, source):

    url_batch = 0
    url_file_path = os.path.join(url_directory, f"urls_{url_batch}.jsonl.zst")
    archiver = Archive(url_file_path)

    current_url = ""
    current_meta = {}
    current_meta["id"] = []
    current_meta["score"] = []
    current_meta["title"] = []
    current_meta["subreddit"] = []
    current_meta["created_utc"] = []

    total_url_count = 0
    url_count = 0
    logger.info("Generating now...")
    for submission_id, url, score, title, subreddit, created_utc in source():
        if not current_url:
            current_url = url
        elif url != current_url:
            # New URL - Add Old URL and meta to archive if score is high enough
            total_score = sum(current_meta["score"])
            if (total_score >= min_score):
                archiver.add_data(current_url, current_meta)
                url_count += 1
                total_url_count += 1

                # Commit and Init New Archive if full
                if url_count == urls_per_file:
                    archiver.commit()
                    url_batch += 1
                    url_file_path = os.path.join(url_directory, f"urls_{url_batch}.jsonl.zst")
                    archiver = Archive(url_file_path)
                    url_count = 0

            current_url = url
            current_meta = {}
            current_meta["id"] = []
            current_meta["score"] = []
            current_meta["title"] = []
            current_meta["subreddit"] = []
            current_meta["created_utc"] = []

        current_meta["id"].append(submission_id)
        current_meta["score"].append(score)
        current_meta["title"].append(title)
        current_meta["subreddit"].append(subreddit)
        current_meta["created_utc"].append(created_utc)

    if url_count > 0:
        archiver.add_data(current_url, current_meta)
        total_url_count += 1        
        archiver.commit()

    url_count_path = os.path.join(url_directory, "url_count.json")
    json.dump(total_url_count, open(url_count_path, "w"))

parser_description = 'Generate URL files from sqlite database containing URLs and reddit metadata.'
parser = argparse.ArgumentParser(description=parser_description)
parser.add_argument("-s", "--start_period", default=None)
parser.add_argument("-f", "--finish_period", default=None)
parser.add_argument("-dir", "--output_directory", default="")
parser.add_argument("--urls_per_file", type=int, default=100000)
parser.add_argument("-score", "--min_score", type=int, default=3)
parser.add_argument("-source", "--data_source", default="db")

if __name__ == '__main__':
    args = parser.parse_args()

    logfile_path = "generate_urls.log"
    setup_logger_tqdm(logfile_path)

    # If both none we can just query all and use the index on url field
    # Otherwise we use the index on created_utc and have to do a costly url sort
    if args.start_period or args.finish_period:
        if args.start_period:
            start_month, start_year = tuple(map(int,args.start_period.split(",")))
            start_date = datetime.datetime(start_year, start_month, 1)
        else:
            start_date = datetime.datetime(2005, 6, 1)

        if args.finish_period:
            finish_month, finish_year = tuple(map(int,args.finish_period.split(",")))
            end_date = datetime.datetime(finish_year, finish_month, 1) 
        else:
            end_date = datetime.datetime.now()

        logger.info(f"Finding URLs between {start_date.strftime('%Y-%m')} and {end_date.strftime('%Y-%m')}")
    else:
        logger.info(f"Finding all URLs.")
        start_date = None
        end_date = None

    urls_directory = os.path.join(args.output_directory, "urls")

    logger.info(f"Urls output directory: {urls_directory}")
    logger.info(f"Minimum score: {args.min_score}")
    logger.info(f"URLs per file: {args.urls_per_file}")

    if args.data_source == "db":
        source = partial(get_from_db, start_date, end_date)
    elif args.data_source == "tsv":
        source = get_from_tsv
    else:
        logger.info(f"Invalid source {args.data_source}")
        sys.exit(-1)

    logger.info(f"Data source: {args.data_source}")    

    generate_urls(urls_directory, args.urls_per_file, args.min_score, source)

