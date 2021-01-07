"""
Called from pushshift_to_sqlite.py.

Processes a PushShift submission dump file, storing the url and relevant Reddit metadata 
into the sqlite database specified in alembic.ini. Note the reddit submission
id is converted from base36 first and the created_utc is stored as a datetime
object.

process_dump_file is the entry point, requiring you to specify 'dump_file_path'
and 'output_directory'. Supports tqdm-multiprocess.

metadata = {}
metadata["id"] = base36.loads(post["id"])
metadata["subreddit"] = post.get("subreddit")
metadata["title"] = post.get("title")
metadata["score"] = post.get("score")
metadata["created_utc"] = datetime.datetime.fromtimestamp(int(post["created_utc"]))
"""

import json
import os
import math
import datetime

import base36
from sqlalchemy import exc

from .models import RedditSubmission
from utils.archive_stream_readers import get_archive_stream_reader

import logging
logger = logging.getLogger()

million = math.pow(10, 6)

chunk_size = int(16 * million) # Must be larger then the size of any post

def process_reddit_post(post):
    is_self = post.get("is_self")
    if is_self is None or is_self:
        return None

    url = post.get("url")
    if url is None or url == "":
        return None

    reddit_submission = RedditSubmission()
    reddit_submission.id = base36.loads(post["id"])
    reddit_submission.subreddit = post.get("subreddit")
    reddit_submission.title = post.get("title")
    reddit_submission.score = post.get("score", 0)
    reddit_submission.created_utc = datetime.datetime.fromtimestamp(int(post["created_utc"]))
    reddit_submission.url = url   

    return reddit_submission

def process_dump_file(dump_file_path, db_session, tqdm_func):
    logging.info(f"Processing dump file '{dump_file_path}'")
    dump_file_size = os.path.getsize(dump_file_path)

    previous_file_position = 0
    count = 0
    insert_batch_size = 100000
    with get_archive_stream_reader(dump_file_path) as reader, \
         tqdm_func(total=dump_file_size, unit="byte", unit_scale=1) as progress:

        progress.set_description(f"Processing {os.path.basename(dump_file_path)}")

        previous_line = ""
        while True:
            chunk = reader.read(chunk_size)
            if not chunk:
                break

            # Update Progress Bar
            current_file_position = reader.tell()
            progress.update(current_file_position - previous_file_position)
            previous_file_position = current_file_position

            # Process chunk + leftover, ignore possibly incomplete last line
            try:
                string_data = chunk.decode("utf-8")
            except UnicodeDecodeError as ex:
                logger.info(f"Error in position {current_file_position} in file {dump_file_path}")
                logger.info(ex)
                continue
            lines = string_data.split("\n")
            for i, line in enumerate(lines[:-1]):
                if i == 0:
                    line = previous_line + line

                reddit_post = None
                try:
                    reddit_post = json.loads(line)
                except Exception as ex:
                    logger.info(f"JSON decoding failed: {ex}")
                    continue
  
                reddit_submission = process_reddit_post(reddit_post)
                if reddit_submission:
                    db_session.add(reddit_submission)
                    count += 1

                    if count == insert_batch_size:    
                        logging.info(f"Committing {count} records to db.")
                        try:
                            db_session.commit()
                        except exc.IntegrityError:
                            logger.info(f"Duplicate INSERT, ignoring.")
                            db_session.rollback()
                        count = 0

            previous_line = lines[-1]

    if count > 0:    
        logging.info(f"Committing {count} records to db.")
        try:
            db_session.commit()
        except exc.IntegrityError:
            logger.info(f"Duplicate INSERT, ignoring.")
            db_session.rollback()
        count = 0

    logging.info("Done with file.")
