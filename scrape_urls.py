import os
import multiprocessing as mpl
import tqdm
import time
from hashlib import sha256
import tldextract
import tarfile
import json
import io

from utils import linecount, chunker, mkdir
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
                return 0
            else:
                return int(batch_id)
    else:
        return 0

def download(url_entry, 
             scraper=newspaper_scraper,
             memoize=False):

    uid, url = url_entry
    url = url.strip()
    fid = "{:07d}-{}".format(uid, sha256(url.encode()).hexdigest())

    # is_good_link, link_type = vet_link(url)
    # if not is_good_link:
    #     return

    text, meta = scraper(url, memoize)

    ext = tldextract.extract(url)
    domain = '.'.join([x for x in ext if x])
    meta["domain"] = domain

    if text is None or text.strip() == "":
        return ("", meta, fid, uid)

    return (text, meta, fid, uid)

def archive_chunk(chunk_id, chunk_data, output_directory, compress_fmt):
    mkdir(output_directory)
    texts, metas, fids, uids = zip(*chunk_data)

    doc_count = 0

    data_file_name = f"{chunk_id}_data.tar"
    data_tar_path = os.path.join(output_directory, data_file_name)
    with tarfile.open(data_tar_path, "w:" + compress_fmt) as tar:
        for text, fid in zip(texts, fids):
            doc_count += 1

            text = text.encode("utf-8")
            tar_info = tarfile.TarInfo(f"{fid}.txt")
            tar_info.size = len(text)
            tar.addfile(tar_info, io.BytesIO(text))

    meta_file_name = f"{chunk_id}_meta.tar"
    meta_tar_path = os.path.join(output_directory, meta_file_name)
    with tarfile.open(meta_tar_path, "w:" + compress_fmt) as tar:
        for meta, fid in zip(texts, fids):
            meta_json = json.dumps(meta)
            meta_json = meta_json.encode("utf-8")
            tar_info = tarfile.TarInfo(f"{fid}.json")
            tar.addfile(tar_info, io.BytesIO(text))

    return doc_count

def scrape_urls(url_file, output_directory, chunk_size, process_count, timeout=-1, compress_fmt="bz2"):
    checkpoint_file = url_file + '.ckpt'
    start_chunk = load_state(checkpoint_file) # Pointless?
    start_elem = start_chunk * chunk_size 

    # URLs we haven't scraped yet (if first run, all URLs in file)
    with open(url_file, "r", encoding="utf-8") as fh:

        total_chunks = linecount(url_file) // chunk_size
        logger.info(f"Total chunks: {total_chunks}")

        url_entries = enumerate(fh)
        chunks = chunker(url_entries, chunk_size, start_elem)

        pool = mpl.Pool(process_count)

        progress = tqdm.tqdm(total=total_chunks)        
        progress.update(start_chunk) # display already-downloaded chunks on progress bar

        # process one "chunk" of chunk_size URLs at a time
        for i, chunk in enumerate(chunks):
            chunk_id = start_chunk + i + 1

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
            count = archive_chunk(chunk_id, chunk_data, output_directory, compress_fmt)
            progress.write("Archive created in {} seconds".format(time.time() - t2))

            # useful_urls = len(list(filter(lambda x: x and x[0], chunk_data)))
            progress.write(f"{count} out of {len(chunk)} URLs yielded content\n")
            progress.update(1)
            save_state(checkpoint_file, chunk_id)

    logger.info("Done!")

def main(logfile_path, url_file, output_directory, chunk_size=1000, process_count=10):
    setup_logger(logfile_path)
    scrape_urls(url_file, output_directory, chunk_size, process_count)    

if __name__ == "__main__":
    logfile_path = "scrape_urls.log"
    url_file = "E:/Eleuther_AI/webtext2/dumps/test/output/RS_2011-01.urls.txt"
    process_count = 30
    chunk_size = 1000
    output_directory = "E:/Eleuther_AI/webtext2/dumps/test/scrapes"

    main(logfile_path, url_file, output_directory, process_count=process_count)
