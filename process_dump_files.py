import json
import tqdm
import os
import glob
import math
import logging


from archive_stream_readers import get_archive_stream_reader

# from filter import should_exclude
from logger import setup_logger

logger = logging.getLogger()

million = math.pow(10, 6)

chunk_size = int(16 * million) # Must be larger then the size of any post

def process_reddit_post(post):
    # print(post.keys())    

    is_self = post.get("is_self")
    if is_self is None or is_self:
        return None

    url = post.get("url")
    if url is None or url == "":
        return None
    else:
        return url   

def process_dump_file(dump_file_path, output_directory):
    dump_file_size = os.path.getsize(dump_file_path) 
    logging.info(f"File Size: {(dump_file_size / million):.2f} MB")

    stats = {}
    stats["post_count"] = 0
    stats["relevant_post_count"] = 0

    dump_file_prefix = "".join(os.path.basename(dump_file_path).split(".")[:-1])

    text_url_file_name = dump_file_prefix + ".urls.txt"
    text_url_path = os.path.join(output_directory, text_url_file_name)
    logger.info(f"Writing URLs to '{text_url_path}'" )

    previous_file_position = 0
    with get_archive_stream_reader(dump_file_path) as reader, \
         open(text_url_path, "w", encoding='utf8') as url_file_handle, \
         tqdm.tqdm(total=dump_file_size, unit="byte", unit_scale=1) as progress:

        progress.set_description("Processing PushShift Dump File")

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
            string_data = chunk.decode('utf-8')
            lines = string_data.split("\n")
            for i, line in enumerate(lines[:-1]):
                if i == 0:
                    line = previous_line + line

                reddit_post = None
                try:
                    reddit_post = json.loads(line)
                except Exception as ex:
                    print("JSON decoding failed: ", ex)
                    continue

                stats["post_count"] += 1
                url = process_reddit_post(reddit_post)
                if url:
                    stats["relevant_post_count"] += 1
                    url_file_handle.write(f"{url}\n") # Actual write to url file

            previous_line = lines[-1]

    # Save Stats File
    stats_file_name = dump_file_prefix + ".stats.json"
    stats_file_path = os.path.join(output_directory, stats_file_name)
    logger.info(f"Dumping stats to '{stats_file_path}'" )
    json.dump(stats, open(stats_file_path,"w"))            

def main(dumps_directory, output_directory):
    os.makedirs(output_directory, exist_ok=True)
    logfile_path = os.path.join(output_directory,"logfile.txt")
    setup_logger(logfile_path)

    logger.info(f"Processing all dump files in '{dumps_directory}'")
    logger.info(f"Output directory: '{output_directory}'")

    files = glob.glob(os.path.join(dumps_directory,"RS_*.zst"))
    files += glob.glob(os.path.join(dumps_directory,"RS_*.bz2"))
    files += glob.glob(os.path.join(dumps_directory,"RS_*.xz"))

    logger.info("Matching file list")
    logger.info("------------------")
    logger.info(files)
    logger.info("------------------")


    for dump_file_path in files:
        logging.info(f"Processing dump file '{dump_file_path}'")

        # Do the work!
        process_dump_file(dump_file_path, output_directory)
        logging.info("Done with file.\n")

if __name__ == '__main__':
    dumps_directory = "E:/Eleuther_AI/webtext2/dumps/test"
    output_directory = "E:/Eleuther_AI/webtext2/dumps/test/output"

    main(dumps_directory, output_directory)