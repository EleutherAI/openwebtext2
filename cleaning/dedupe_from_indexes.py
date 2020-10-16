"""
This script builds a list of all duplicates by file_id & document_id, and then iterates
through all ".minscored" files from the filename lookup, creating a new archive for each 
file in the original containing all documents that were not marked as duplicates during 
the previous step.

So for each original file, a "_final.jsonl.zst" files will be output in the original
directory.

Arguments
------
--batch_directory (-dir)
    Directory containing the "*duplicates.txt" files along with the "file_name_lookup.pkl"
    created during batch slicing. The "_final.jsonl.zst" files will be output in their
    original directories.
"""

import glob
import os
import pickle
import argparse

import tqdm

from utils.archiver import Archive, Reader

import logging
from utils.logger import setup_logger_tqdm
logger = logging.getLogger(__name__)

def main(batch_directory):
    file_name_lookup_path = os.path.join(batch_directory, "file_name_lookup.pkl")
    file_name_lookup = pickle.load(open(file_name_lookup_path,"rb"))

    logger.info("Building duplicates dictionary...")
    duplicates_dict = {file_id : set() for file_id in range(len(file_name_lookup))}
    duplicate_files = glob.glob(os.path.join(batch_directory, "*_duplicates.txt"))
    for duplicate_file in duplicate_files:
        with open(duplicate_file, "r") as fh:
            duplicates = fh.read().splitlines()
            for duplicate in duplicates:
                file_id, document_id = tuple(map(int, duplicate.split(" ")))                
                duplicates_dict[file_id].add(document_id)

    logger.info("De-duplicating files...")    
    for file_id, original_file_name in enumerate(tqdm.tqdm(file_name_lookup)):
        final_file_name = original_file_name.replace("_default.jsonl.zst.deduped.merged.minscored",
                                                     "_final.jsonl.zst")

        reader = Reader()
        count = 0
        archiver = Archive(final_file_name)
        for document, metadata in reader.read_jsonl(original_file_name, get_meta=True):
            if count not in duplicates_dict[file_id]:
                archiver.add_data(document, metadata)
            count += 1
        archiver.commit()

parser = argparse.ArgumentParser(description='Dedupe from provided indexes.')
parser.add_argument("-dir", "--batch_directory", default="")

if __name__ == '__main__':
    logfile_path = "dedupe_from_index.log"
    setup_logger_tqdm(logfile_path)

    args = parser.parse_args()
    main(args.batch_directory)