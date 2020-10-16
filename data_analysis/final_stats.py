import os
import glob
import argparse
import pickle
import math

import tqdm

from utils.archiver import Reader

import logging
from utils.logger import setup_logger_tqdm
logger = logging.getLogger(__name__)

def get_stats_old():
    batch_directory = "/home/researcher2/webtext2/test"
    files = glob.glob(os.path.join(batch_directory, "*_duplicates.txt"))
    duplicate_count = 0
    for file_path in files:
        with open(file_path, "r") as fh:
            duplicate_count += len(fh.readlines())

    document_count_path = os.path.join(batch_directory, "document_count.pkl")
    document_count = pickle.load(open(document_count_path, "rb"))

    print("Total Duplicates: ", duplicate_count)
    print("Original Documents: ", document_count)

    useful_percentage = (1 - duplicate_count / document_count) * 100
    print(f"Useful Data: {useful_percentage:0.2f}%")

def get_stats(final_directory):

    reader = Reader()
    files = glob.glob(os.path.join(final_directory, "*jsonl.zst"))

    document_count = 0
    total_text_size = 0   
    logger.info("Getting final document count and total uncompressed text size.")    
    for file_path in tqdm.tqdm(files, dynamic_ncols=True):
        for document, metadata in reader.read_jsonl(file_path, get_meta=True):
            document_count += 1
            total_text_size += len(document)

    return document_count, total_text_size

parser = argparse.ArgumentParser(description='Final statistics')
parser.add_argument("-dir", "--final_directory", default="")

if __name__ == '__main__':
    logfile_path = "final_statistics.log"
    setup_logger_tqdm(logfile_path)

    args = parser.parse_args()

    final_count, total_text_size = get_stats(args.final_directory)
    billion = math.pow(10, 9)
    logger.info(f"Final Document Count: {final_count:,}")
    print(f"Total uncompressed text size: {(total_text_size / billion):.2f} GB")
    pickle.dump((final_count, total_text_size), open("final_stats.pkl", "wb"))