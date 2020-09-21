import glob
import tqdm
from functools import reduce
from operator import add
import os
import math

million = math.pow(10, 6)

def url_dedupe(urls_directory):

    files = glob.glob(os.path.join(urls_directory, "*urls.txt"))
    total_file_size = reduce(add, map(os.path.getsize, files))
    print(f"Total File Size: {(total_file_size / million):.2f} MB")
    progress = tqdm.tqdm(total=total_file_size, dynamic_ncols=True, unit_scale=1)

    seen_urls = set()
    duplicate_count = 0
    document_count = 0
    for url_file_path in files:
        url_file_name_out = f"{os.path.basename(url_file_path).replace('.urls.txt', '')}_deduped.urls.txt"
        url_file_path_out = os.path.join(urls_directory, url_file_name_out)
        with open(url_file_path, "r", encoding="utf-8") as fh_in, \
             open(url_file_path_out, "w", encoding="utf-8") as fh_out:

            print(f"Processing File '{url_file_path}'")
            url_entries = fh_in.readlines()
            for url in url_entries:
                document_count += 1

                if url in seen_urls:  
                    duplicate_count +=1
                    continue

                seen_urls.add(url)
                fh_out.write(url)
        progress.update(os.path.getsize(url_file_path))

    progress.close()

    print("Document Count: ", document_count)
    print("Duplicate Count: ", duplicate_count)
    useful_percentage = (1 - duplicate_count / document_count) * 100
    print(f"Useful Data: {useful_percentage:0.2f}%")

if __name__ == '__main__':
    urls_directory = "E:/Eleuther_AI/webtext2/dumps/urls"
    url_dedupe(urls_directory)
