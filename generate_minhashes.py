import glob
import os
import math
import pickle
from functools import reduce
from operator import add
#from pqdm.processes import pqdm Doesn't seem to work on windows
import multiprocessing
from queue import Empty as EmptyQueue # filth
import signal
from contextlib import redirect_stdout
from signal import SIGINT
import sys

import tqdm
import nltk
from nltk.util import ngrams
from datasketch import MinHash, MinHashLSH

from utils import Timer

import logging
from logger import setup_logger
logger = logging.getLogger(__name__)

million = math.pow(10, 6)

import lm_dataformat
import io
import zstandard
import jsonlines
def read_jsonl(self, file, get_meta=False, autojoin_paragraphs=True, para_joiner='\n\n'):
    with open(file, 'rb') as fh:
        self.fh = fh
        cctx = zstandard.ZstdDecompressor()
        reader = io.BufferedReader(cctx.stream_reader(fh))
        rdr = jsonlines.Reader(reader)
        for ob in rdr:
            # naive jsonl where each object is just the string itself, with no meta. For legacy compatibility.
            if isinstance(ob, str):
                assert not get_meta
                yield ob
                continue

            text = ob['text']

            if autojoin_paragraphs and isinstance(text, list):
                text = para_joiner.join(text)

            if get_meta:
                yield text, (ob['meta'] if 'meta' in ob else {})
            else:
                yield text

lm_dataformat.Reader.read_jsonl = read_jsonl

def extract_ngrams(data, num):
    n_grams = ngrams(nltk.word_tokenize(data), num)
    return [ ' '.join(grams) for grams in n_grams]

# Signal handling for multiproces. The "correct" answer doesn't work on windows at all.
# Using the version with a very slight race condition. Don't ctrl-c in that miniscule time window...
# https://stackoverflow.com/questions/11312525/catch-ctrlc-sigint-and-exit-multiprocesses-gracefully-in-python
def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

terminate = False
def handler(signal_received, frame):
    global terminate
    terminate = True

# Multiprocessed
def process_file(file_name, progress_queue, min_grams_queue):
    lm_reader = lm_dataformat.Reader("")
    min_grams = []
    previous_file_position = 0
    logger.info(f"Processing File '{file_name}'")
    for document, metadata in lm_reader.read_jsonl(file_name, get_meta=True):
        logger.info("document!")

        # Update Progress Bar In Main Process
        current_file_position = lm_reader.fh.tell()
        progress_queue.put(current_file_position - previous_file_position)
        previous_file_position = current_file_position

        n_grams = extract_ngrams(document, 5)
        five_gram_set = set(n_grams)
        minhash = MinHash(num_perm=128)
        for five_gram in five_gram_set:
            minhash.update(five_gram.encode('utf8'))
        min_grams.append(minhash)

    min_grams_queue.put((file_name, min_grams))

def generate_minhashes(scrape_directory, output_pickle_path):
    signal.signal(SIGINT, handler)

    # [(file_name, [doc0_minhash, doc1_minhash, ...]), ....]
    minhashes_by_file = []
    with multiprocessing.Pool(4, init_worker) as pool:
        files = glob.glob(f'{scrape_directory}/*jsonl.zst')

        total_file_size = reduce(add, map(os.path.getsize, files))
        logger.info(f"Total File Size: {(total_file_size / million):.2f} MB")

        progress = tqdm.tqdm(total=total_file_size, dynamic_ncols=True, unit_scale=1)

        m = multiprocessing.Manager()
        progress_queue = m.Queue()
        min_grams_queue = m.Queue()

        #for file_name in files:
        #    process_file(file_name, progress_queue, min_grams_queue)

        multiple_results = [pool.apply_async(process_file, (file_name, progress_queue, min_grams_queue)) for file_name in files]

        countdown = len(files)
        while countdown > 0 and not terminate:
            try:
                while True:
                    progress.update(progress_queue.get_nowait())
            except EmptyQueue:
                pass
            except InterruptedError: # get_nowait will error if interrupted by signal
                pass

            try:
                minhashes_by_file.append(min_grams_queue.get_nowait())
                countdown -= 1
            except EmptyQueue:
                pass
            except InterruptedError: # get_nowait will error if interrupted by signal
                pass

        if terminate:
            print()
            print('SIGINT or CTRL-C detected, killing pool')
            sys.exit(0)        

    return minhashes_by_file

if __name__ == '__main__':

    with redirect_stdout(open(os.devnull, "w")):
        nltk.download('punkt')

    log_file = "generate_minhashes.log"
    setup_logger(log_file)

    scrape_directory = "E:\Eleuther_AI\webtext2\scrapes"
    output_pickle_path = "E:\Eleuther_AI\webtext2\dedupe\minhashes.pkl"
    
    logger.info("Generating document level minhashes from 5 gram sets")
    minhashes_by_file = generate_minhashes(scrape_directory, output_pickle_path)
    
    logger.info("Pickling minhashes_by_file")
    timer = Timer().start()
    pickle.dump(minhashes_by_file, open(output_pickle_path,"wb"))
    logger.info(timer.stop_string())
    
