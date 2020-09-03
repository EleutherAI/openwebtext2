# PushShift Dump Processing

This project is part of Eleuther AI's quest to create a massive repository of text data for training language models. The WebText dataset used for training GPT2 contains websites scraped from all reddit posts between a certain time range. Our aim is to extend this to all reddit posts ever made and we are naming this effort WebText2.

PushShift provides dumps of all reddit posts and submissions, but they are normally a few months out of date. This project processes these files. 

If we require the most recent reddit posts for any reason another project will be required to create a live scraping tool as even the PushShift API is not reliable for recent data.

**Acknowledgements**  
Much of this code was written by @hohohoho (researcher2 on github), with inspiration and some straight copying of the scraping code found at https://github.com/yet-another-account/openwebtext/

**Environment Setup**  
Tested in a basic conda environment, though conda probably isn't necessary.

```conda create --name pushshift_dump_processing python=3.8```

All requirements can be installed with: 

```pip install -r requirements.txt```

There are three parts in this pipeline so far:

1. Download the compressed pushshift dumps
2. Process the downloaded dump files
3. Scrape the URLs sourced from step 2



## Part 1 - Downloading Compressed Dump Files

This is done within **download_pushshift_dumps.py**.

As the pushshift dumps shifted between various compressions formats the program cycles through all possibilities until it finds a matching file for a given month, preventing any duplicates from being downloaded.

To run, either change the hardcoded parameters inside of __name__ == '__main__':, or call the main method from another program. 

A date range on the main method allows you to do a single month at a time for a concurrent pipeline or if disk space is an issue.

The following example will download all pushshift dumps up until the most recent into the *dumps* subfolder of the current path. Log can be found in *./download_pushshift_dumps.log*.

```python
if __name__ == '__main__':
    logfile_path = "download_pushshift_dumps.log"
    
    start_date = datetime.datetime(2011, 1, 1)    
    end_date = datetime.datetime.now()

    download_directory = "dumps"

    main(logfile_path, start_date, end_date, download_directory) 
```
 

## Part 2 - Processing The Downloaded Dumps

This is done within **process_dump_files.py**.

Either change the hardcoded parameters inside of __name__ == '__main__':, or call the main method from another program. 

The following example will process all dump files found within the *dumps* directory - filename matching "RS_*" for the relevant compression formats. For each matching file a separate *name.stats.json* and *name.urls.txt* will be created in the output directory.

Currently we support the three different compression formats provided by PushShift - bz2, xz, zst.

```python
if __name__ == '__main__':
    dumps_directory = "dumps"
    output_directory_root = "output"

    main(dumps_directory, output_directory_root)
```

## Part 3 - Scraping From Sourced URLs

This is done within **scrape_urls.py**. 

Either change the hardcoded parameters inside of __name__ == '__main__':, or call the main method from another program.

This program iterates through a URL file generated in step 2 above. It does a single URL file at a time and will take many hours. It loads batches of URLs and hands them out to worker processes to scrape using newspaper scraper. It will archive each scraped batch using bz2 tar, one file for text and one for metadata. You can modify the batch size and process count depending on your environment.

If you want to filter the urls before scraping we have left an example filter in **filter.py**. You will need to modify **scrape_urls.py** where the url_file is loaded to implement this.

NOTE: The program saves a checkpoint file in the same directory as the url.txt file to allow you to resume later if the job dies or needs to be killed

The following example will scrape all URLs found in file found at *output/RS_2020-04/urls.txt*

```python
if __name__ == "__main__":
    logfile_path = "scrape_urls.log"
    url_file = "output/RS_2011-01.urls.txt"
    process_count = 30
    chunk_size = 1000
    output_directory = "scrapes"

    main(logfile_path, url_file, output_directory, process_count=process_count)
```