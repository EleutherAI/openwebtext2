"""
This script uses MinHashLSH from the datasketch library to deduplicate
across the whole set of document minhashes.

See http://ekzhu.com/datasketch/lsh.html for more details.

We use the Cassandra backend to keep memory usage low and a 0.5 threshold
for duplicate detection.

In it's current state, the script creates and pickles a MinHashLSH object
containing the parameters for a local cassandra instance. If you want 
a remote instance you could either change the script or perform port
redirection to a remote machine with SSH tunelling for example. This uses
the default Cassandra port.

lsh = MinHashLSH(
    threshold=0.5, num_perm=10, storage_config={
        'type': 'cassandra',
        'cassandra': {
            'seeds': ['127.0.0.1'],
            'keyspace': 'minhash_lsh_keyspace',
            'replication': {
                'class': 'SimpleStrategy',
                'replication_factor': '1',
            },
            'drop_keyspace': False,
            'drop_tables': False,
        }
    }
)

Importantly, once you run the script you must keep using the same lsh object
if you want the same basenames for the created Cassandra tables.

We are running into issues with loading this MinHashLSH object in multiple processes
so did our run with just a single process. The first run will freeze when trying
to unpickle the MinHashLSH object in the worker process. On running a second time it 
will work.

We save file and document level checkpoints after each query to allow for easy resuming.
Each batch file will have a corresponding "*_duplicates.txt" when done.

Arguments
------
--batch_directory (-dir)
    Directory containing the "batch*.pkl" files. "lsh.pkl", duplicate lists and
    batch checkpoints will be saved here. 
--process_count (-procs)
    Number of processes in the pool. Defaults to 1.
"""

import os
import glob
import argparse
import json
import pickle

import tqdm
from datasketch import MinHashLSH
from tqdm_multiprocess import TqdmMultiProcessPool

from utils.utils import Timer, timed_pickle_dump, timed_pickle_load

import logging
from utils.logger import setup_logger_tqdm
logger = logging.getLogger(__name__)

def get_minhash_lsh_cassandra():
    lsh = MinHashLSH(
        threshold=0.5, num_perm=10, storage_config={
            'type': 'cassandra',
            'basename': b'owt2',
            'cassandra': {
                'seeds': ['127.0.0.1'],
                'keyspace': 'minhash_lsh_keyspace',
                'replication': {
                    'class': 'SimpleStrategy',
                    'replication_factor': '1',
                },
                'drop_keyspace': False,
                'drop_tables': False,
            }
        }
    )
    return lsh

def minhash_lsh_dedupe_cassandra(batch_minhashes_pickle_path, lsh_pickle_path, tqdm_func, global_tqdm):
    # [(file_id, [doc0_minhash, doc1_minhash, ...]), ....]
    batch_minhashes = timed_pickle_load(batch_minhashes_pickle_path, "batch minhashes")

    # For some reason this will freeze when loading on the first run. 
    lsh = get_minhash_lsh_cassandra()

    checkpoint_file = batch_minhashes_pickle_path.replace(".pkl","_ckpt.pkl")
    if os.path.exists(checkpoint_file):
        ckpt_file_id, ckpt_document_id = pickle.load(open(checkpoint_file,"rb"))
    else:
        ckpt_file_id = -1
        ckpt_document_id = -1

    logger.info("Detecting duplicates")
    timer = Timer().start()
    duplicate_file_path = batch_minhashes_pickle_path.replace(".pkl", "_duplicates.txt")    
    with open(duplicate_file_path, "a") as fh:
        for file_id, documents in batch_minhashes:
            if file_id <= ckpt_file_id:
                global_tqdm.update(len(documents))
                continue
            for document_id, minhash in enumerate(documents):            
                if document_id <= ckpt_document_id:
                    global_tqdm.update(ckpt_document_id + 1)
                    ckpt_document_id = -1
                    continue
                results = lsh.query(minhash)
                duplicate_found = True if results else False
                is_self = False
                for json_results in results:
                    found_file_id, found_document_id = json.loads(json_results)
                    # This check is needed in case you re-run things
                    if file_id == found_file_id and document_id == found_document_id:
                        duplicate_found = False
                        is_self = True
                        break

                if duplicate_found:
                    fh.write(f"{file_id} {document_id}\n")
                else:
                    if not is_self:
                        lsh.insert(json.dumps((file_id, document_id)), minhash)

                global_tqdm.update()
                pickle.dump((file_id, document_id), open(checkpoint_file,"wb"))

    logger.info(timer.stop_string())

    return True

import time

def main(process_count, batch_directory):

    # # Ensure LSH object containing cassandra connection info exists
    # lsh_pickle_path = os.path.join(batch_directory, "lsh.pkl")
    # if not os.path.exists(lsh_pickle_path):
    #     logger.info("Getting cassandra minhash lsh")
    #     lsh = get_minhash_lsh_cassandra()
    #     timed_pickle_dump(lsh, lsh_pickle_path, "lsh")

    # Initialize to avoid race conditions?
    lsh = get_minhash_lsh_cassandra()
    time.sleep(5)

    files = glob.glob(os.path.join(batch_directory, "batch*.pkl"), recursive=True)

    pool = TqdmMultiProcessPool()
    tasks = []

    document_count_path = os.path.join(batch_directory, "document_count.pkl")
    total_documents = pickle.load(open(document_count_path,"rb"))

    for batch_file in files:
        arguments = (batch_file, None)
        task = (minhash_lsh_dedupe_cassandra, arguments)
        tasks.append(task)

    on_done = lambda _ : logger.info("done")
    on_error = lambda _ : logger.info("error")
    with tqdm.tqdm(total=total_documents, dynamic_ncols=True) as progress:
        result = pool.map(process_count, progress, tasks, on_error, on_done)
        logger.info(result)

parser = argparse.ArgumentParser(description='Minhash LSH dedupe with cassandra backend.')
parser.add_argument("-dir", "--batch_directory", default="")
parser.add_argument("-procs", "--process_count", type=int, default=1)

if __name__ == '__main__':
    logfile_path = "minhash_lsh_dedupe.log"
    setup_logger_tqdm(logfile_path)

    args = parser.parse_args()

    main(args.process_count, args.batch_directory)