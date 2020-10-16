"""
Splits minhashes.pkl into approximately the desired number of batches.
As we always split on file lines this won't always be exact unless all
files have the same number of documents.

The "directory" must contain a 'minhashes.pkl' file created with 'generate_minhashes.py'.

Produces batch files named 'batch0.pkl, batch1.pkl ...'. They contain the following
pickled data structure:
[(file_id, [doc0_minhash, doc1_minhash, ...]), ....]

Produces a file name lookup named 'file_name_lookup.pkl'. Contains the following
pickled data structure:
[file_name1, file_name2, file_name3, ...]

Arguments
------
--directory (-dir)
    Directory containing the 'minhashes.pkl' file. Batch files and
    file name lookup will be saved here.
--number_of_batches (-batches)
    Approximate number of batches to split minhashes into.
"""

import os
import argparse
import pickle

import tqdm
from utils.utils import timed_pickle_dump, timed_pickle_load

import logging
from utils.logger import setup_logger_tqdm
logger = logging.getLogger(__name__)

def main(number_of_batches, batch_directory):
    minhashes_pickle_path = os.path.join(batch_directory, "minhashes.pkl")

    # [(file_name, [doc0_minhash, doc1_minhash, ...]), ....]    
    minhashes = timed_pickle_load(minhashes_pickle_path, "minhashes")

    logger.info("Splitting minhashes for batching...")
    total_documents = 0
    for _ , documents in minhashes:
        total_documents += len(documents)

    document_count = 0
    documents_per_batch = total_documents / number_of_batches
    current_batch = []
    batch_count = 0
    for file_id, (file_name, documents) in tqdm.tqdm(enumerate(minhashes)):
        document_count += len(documents)
        current_batch.append((file_id, documents)) # Note we only store globally unique file_id here

        if document_count > (batch_count + 1) * documents_per_batch:
            batch_pickle_file_path = os.path.join(batch_directory, f"batch{batch_count}.pkl")
            timed_pickle_dump(current_batch, batch_pickle_file_path, f"batch {batch_count} minhashes")
            current_batch = []
            batch_count += 1

    if current_batch:
        batch_pickle_file_path = os.path.join(batch_directory, f"batch{batch_count}.pkl")
        timed_pickle_dump(current_batch, batch_pickle_file_path, f"batch {batch_count} minhashes") 
        current_batch = None

    file_name_lookup = [file_name for file_name, documents in minhashes]
    file_name_lookup_path = os.path.join(batch_directory, "file_name_lookup.pkl")
    timed_pickle_dump(file_name_lookup, file_name_lookup_path, "Filename lookup")

    document_count_path = os.path.join(batch_directory, "document_count.pkl")
    pickle.dump(total_documents, open(document_count_path,"wb"))


parser = argparse.ArgumentParser(description='Generate batches of minhashes for cassandra lsh dedupe.')
parser.add_argument("-dir", "--directory", default="")
parser.add_argument("-batches", "--number_of_batches", type=int, required=True)

if __name__ == '__main__':
    logfile_path = "minhash_lsh_batching.log"
    setup_logger_tqdm(logfile_path)

    args = parser.parse_args()    

    main(args.number_of_batches, args.directory)