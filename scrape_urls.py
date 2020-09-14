import os
import multiprocessing as mpl
import tqdm
import time
from hashlib import sha256
import tldextract
import lm_dataformat
import sys

from utils import linecount, chunker
from scrapers import newspaper_scraper

import logging
from logger import setup_logger

logger = logging.getLogger()

def save_state(checkpoint_file, chunk_id):
    with open(checkpoint_file, 'w') as fp:
        fp.write(str(chunk_id))

def load_state(checkpoint_file):
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as fp:
            batch_id = fp.read()
            if batch_id == '':
                return -1
            else:
                return int(batch_id)
    else:
        return -1

def download(url_entry, 
             scraper=newspaper_scraper,
             memoize=False):

    uid, url = url_entry
    url = url.strip()

    # Original script had this, we no longer use - left for future reference
    # fid = "{:07d}-{}".format(uid, sha256(url.encode()).hexdigest())

    # is_good_link, link_type = vet_link(url)
    # if not is_good_link:
    #     return

    text, meta = scraper(url, memoize)

    # Add top level domain to meta (already includes url among other things)
    ext = tldextract.extract(url)
    domain = '.'.join([x for x in ext if x])
    meta["domain"] = domain

    if text is None or text.strip() == "":
        return ("", meta)

    return (text, meta)

def archive_chunk(chunk_data, chunk_id, lm_archiver):      
    lm_archiver.i = chunk_id
    count = 0
    for (text, meta) in chunk_data:
        if text:
            count += 1
            lm_archiver.add_data(text, meta)
           
    lm_archiver.commit() 

    return count

def scrape_urls(url_file_path, output_directory_path, chunk_size, process_count, timeout=60):
    checkpoint_file = url_file_path + '.ckpt'
    start_chunk = load_state(checkpoint_file) + 1
    start_elem = start_chunk * chunk_size
    lm_archiver = lm_dataformat.Archive(output_directory_path)

    # URLs we haven't scraped yet (if first run, all URLs in file)
    with open(url_file_path, "r", encoding="utf-8") as fh:

        total_chunks = linecount(url_file_path) // chunk_size
        logger.info(f"Total chunks: {total_chunks}")

        url_entries = enumerate(fh)
        chunks = chunker(url_entries, chunk_size, start_elem)

        pool = mpl.Pool(process_count)

        progress = tqdm.tqdm(total=total_chunks)        
        progress.update(start_chunk) # display already-downloaded chunks on progress bar

        # process one "chunk" of chunk_size URLs at a time
        for i, chunk in enumerate(chunks):
            chunk_id = start_chunk + i

            progress.write("Downloading chunk {}".format(chunk_id))
            t1 = time.time()

            # I haven't tested timeout - copied from other codebase for future use
            if timeout > 0:
                # imap as iterator allows .next() w/ timeout.
                # ordered version doesn't seem to work correctly.
                # for some reason, you CANNOT track j or chunk[j] in the loop,
                # so don't add anything else to the loop below!
                # confusingly, chunksize below is unrelated to our chunk_size
                chunk_iter = pool.imap_unordered(download, chunk, chunksize=1)
                chunk_data = []
                for j in range(len(chunk)):
                    try:
                        result = chunk_iter.next(timeout=timeout)
                        chunk_data.append(result)
                    except mpl.TimeoutError:
                        progress.write("   --- Timeout Error ---   ")
            else:
                # chunksize here is for pool.imap, not our chunks
                chunk_data = list(pool.imap(download, chunk, chunksize=1))

            progress.write("{} / {} downloads timed out".format(len(chunk) - len(chunk_data), len(chunk)))
            progress.write("Chunk time: {} seconds".format(time.time() - t1))

            # archive and save this chunk to file
            progress.write("Compressing...")
            t2 = time.time()
            count = archive_chunk(chunk_data, chunk_id, lm_archiver)
            progress.write("Archive created in {} seconds".format(time.time() - t2))

            # useful_urls = len(list(filter(lambda x: x and x[0], chunk_data)))
            progress.write(f"{count} out of {len(chunk)} URLs yielded content\n")
            progress.update(1)
            save_state(checkpoint_file, chunk_id)

    logger.info("Done!")

def main(logfile_name, url_file_path, output_directory_root, chunk_size=1000, process_count=10):
    new_directory_name = os.path.basename(url_file_path).lower().replace(".urls.txt","")
    output_directory_path = os.path.join(output_directory_root, new_directory_name)
    os.makedirs(output_directory_path, exist_ok=True)

    logfile_path = os.path.join(output_directory_path, logfile_name)
    setup_logger(logfile_path)
    logger.info(f"Output Directory: '{output_directory_path}'") 

    scrape_urls(url_file_path, output_directory_path, chunk_size, process_count)    

if __name__ == "__main__":
    logfile_name = "scrape_urls.log"
    url_file_path = "E:/Eleuther_AI/webtext2/dumps/urls/RS_2011-01.urls.txt"
    process_count = 60
    chunk_size = 10000
    output_directory = "E:/Eleuther_AI/webtext2/dumps/scrapes"

    # Override
    if len(sys.argv) == 3:
        url_file_path = sys.argv[1]
        output_directory = sys.argv[2]

    main(logfile_name, url_file_path, output_directory, chunk_size=chunk_size, process_count=process_count)
