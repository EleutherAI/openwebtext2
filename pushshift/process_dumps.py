import subprocess
import os
import argparse

import logging
from utils.logger import setup_logger_tqdm
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description='Download PushShift submission dumps, extra urls')
parser.add_argument("-dir", "--output_directory", default="")
parser.add_argument("-kd", "--keep_dumps", action='store_true')

def main():
    logfile_path = "process_dumps.log"
    setup_logger_tqdm(logfile_path) # Logger will write messages using tqdm.write

    args = parser.parse_args()

    dumps_directory = os.path.join(args.output_directory, "dumps")
    temp_json = os.path.join(args.output_directory, "urls.dat")
    sorted_json = os.path.join(args.output_directory, "urls_sorted.dat")

    command = f'find {dumps_directory} -name "*.bz2" | xargs bzcat | jq -c -r "[.url, .id, .subreddit, .title, .score, .created_utc] | @tsv" >> {temp_json}'
    logger.info(command)
    subprocess.call(command, shell=True)

    command = f'find {dumps_directory} -name "*.xz" | xargs xzcat | xargs jq -c -r "[.url, .id, .subreddit, .title, .score, .created_utc] | @tsv" >> {temp_json}'
    logger.info(command)
    subprocess.call(command, shell=True)

    command = f'find {dumps_directory} -name "*.zst" | xargs zcat | xargs jq -c -r "[.url, .id, .subreddit, .title, .score, .created_utc] | @tsv" >> {temp_json}'
    logger.info(command)
    subprocess.call(command, shell=True)

    command = f'cat {temp_json} | sort -k 1 > {sorted_json}'
    logger.info(command)    
    subprocess.call(command, shell=True)

if __name__ == '__main__':    
    main()  
