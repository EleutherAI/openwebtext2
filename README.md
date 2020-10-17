# OpenWebText2

This project is part of Eleuther AI's quest to create a massive repository of high quality text data for training language models.

**Acknowledgements**  
Much of this code was written by @researcher2, with inspiration and some straight copying of the scraping code found [here](https://github.com/yet-another-account/openwebtext/). @sdtblck kindly put together the Colab notebook, and performed a chunk of the scraping. @leogao2 provided overall design guidance, lm_dataformat, and performed another chunk of scraping. Thanks to [Colaboratory](https://colab.research.google.com/) for the vms, they helped us with about 10% of our overall scraping.

## TLDR (Just Give Me Datas)
Please checkout [lm_dataformat](https://github.com/leogao2/lm_dataformat) for reading the files, or look at our slightly modified version included in utils/archiver.py. Be sure to call read_jsonl with get_meta=True as both versions contain useful metadata for each document, including several original Reddit fields.

### Plug and Play Version
Deduplicated by URL. Filtered by minimum reddit score 3. Deduplicated at document level with MinHashLSH.

Contains 17,103,059 documents for a total of 65.86gb uncompressed text.  
openwebtext2_pnp.tar (28gb compressed including text and metadata) **currently uploading**

### Raw Scrapes
Only deduplicated by URL.

Contains 69,547,149 documents for a total of 193.89gb uncompressed text.  
openwebtext2_raw.tar (79gb compressed including text and metadata) **currently uploading**

## Background

[OpenAI](https://openai.com/) required around 40gb of high quality text corpus for training [GPT2](https://openai.com/blog/better-language-models/). Common Crawl provides the scale necessary for modern language models, however the quality is unreliable. Manual curation of Common Crawl is always an option, albeit an expensive one. Thankfully Reddit provides high quality decentralized curation by design, and this became the key innovation for the WebText dataset.

The generation of WebText can be summarized as:
1. Scrape URLs from all Reddit submissions up to and including December 2017 with 3 or higher score.
2. Deduplicate scraped content based on URL
3. Exclude wikipedia (OpenAI already had a separate Wikipedia dataset)
4. Deduplicate remaining content using undisclosed "heuristic based cleaning". This includes removal of non-english web pages.

Neither the resulting corpus or generation source code was made public, inspiring Aaron Gokaslan and Vanya Cohen to create the [OpenWebTextCorpus](https://skylion007.github.io/OpenWebTextCorpus/).

OpenWebTextCorpus is an open source reproduction of WebText, reifying the "heuristic based cleaning" stage using fuzzy deduplication and enforcing a minimum token length. For content based de-duplication they used local-sensitivity-hashing (LSH) with minhash on sets of 5-grams at the document level. Documents were then tokenized and any with less then 128 tokens were removed. After all processing there remained 40GB of text across 8,013,769 documents. The original code is unavailable at this time, but there are several popular repositories that cover the pipeline to various degrees.

## OpenWebText2 Motivation

Our primary goals for the corpus are:

1. More data! Coverage of the original OpenWebTextCorpus ended at December 2017.
2. Include all languages, providing metadata for easy filtering
3. Provide several versions of the generated corpus for differing user requirements. Both versions will be broken up by month and frozen, with future months available once PushShift submission dumps become available.
    * Raw version containing all scraped pages with associated Reddit submission metadata
    * Plug and play version based on submissions of minimum 3 score with content based fuzzy de-duplication
4. Provide full source code for all stages of the pipeline including deduplication.

Some additional stretch goals:
1. PyTorch dataset
2. TensorFlow dataset

We decided on a rewrite taking inspiration from both https://github.com/yet-another-account/openwebtext/ and https://github.com/jcpeterson/openwebtext.

## The Pipeline

[PushShift](https://www.reddit.com/r/pushshift/comments/bcxguf/new_to_pushshift_read_this_faq/) provides dumps of all reddit posts and submissions, however they are normally a few months behind. While this would be problematic for certain use cases, we don't require up to the minute data for training GPTNeo. For the initial stage of this project we decided to avoid scraping more recent Reddit submissions either directly or via APIs. We may add this in the future.

At a high level the pipeline is broken down as follows:

1. Download Reddit submission dump files from PushShift
2. Process the files to extract URLs from all non-self submissions. Save URLs and Reddit metadata with [lm_dataformat](https://github.com/leogao2/lm_dataformat)
3. Deduplicate the URLs
4. Scrape the URLs using [Newspaper3k](https://newspaper.readthedocs.io/en/latest/), saving both text and metadata using lm_dataformat
5. Filter the scraped documents by minimum Reddit score 3.
6. Perform fuzzy deduplication using [MinHashLSH](http://ekzhu.com/datasketch/lsh.html)

## Environment Setup
Tested in a basic conda environment, though you could use venv or even the global python environment if you wish. I use miniconda to avoid a bloated download.

**Miniconda Install For Linux**

Follow the below steps, or read the conda instructions if you wish: https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html

```bash
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
sha256 Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
```
Select yes on the init step.

Restart your shell to refresh the path.

**Create and activate conda environment**

Environments are saved in a central store on the local disk, no need to create folders like with venv.
```
conda create --name pushshift python=3.8
conda activate pushshift
```

**Install Repo and Requirements**
```bash
git clone https://github.com/EleutherAI/pushshift_dump_processing
cd pushshift_dump_processing
pip install -r requirements.txt
```

## Processing Instructions
Broadly there are five stages in this pipeline:

1. Processing the PushShift submission dumps
2. Scraping the extracted URLs
3. Filtering the scraped doucuments by aggregate minimum Reddit score
4. De-duplicating the filtered documents with MinHashLSH
5. Packaging up the various dataset releases
6. Produce some useful stats about our releases

For all steps below we assume you have completed the environment setup.

## Stage 1 - Processing PushShift Submission Dumps

This is broken down into the following steps:

1. Download and verify the PushShift submission dumps, extracting and storing urls and metadata
from relevant submissions into the sqlite database. Performed by "pushshift/pushshift_to_sqlite.py".
2. Query the populated sqlite database and build a list of URLs with metadata for all related submissions.
Performed by "pushshift/generate_urls_from_sqlite.py"

All scripts for this stage can be found within the "pushshift" package.

### Initial Preparation
You will first need to create a working directory on a drive with plenty of space (500gb recommended). Then configure your sqlite database settings as follows:

Inside the cloned repo:
```bash
cp alembic.ini.template alembic.ini
```
Modify the following line within the alembic.ini file. For example on windows:
```
sqlalchemy.url = sqlite:///e:/Eleuther_AI/webtext2/submissions.sqlite
```
Or on Linux (notice the extra forward slash before "mnt" to indicate system root):
```
sqlalchemy.url = sqlite:////mnt/data/openwebtext2/submissions.sqlite
```

### PushShift Submission Dump Data -> Sqlite DB

This step is performed by "pushshift/pushshift_to_sqlite.py". The script accepts the following arguments:

--start_period (-s)  
    Month and Year of first pushshift dump. Default: 6,2005  
--finish_period (-f)  
    Month and Year of final pushshift dump. Defaults to current month, ignoring any missing months.  
--output_directory (-dir)  
    Base directory that will contain the dumps subdirectory created as part of the process.  
--keep_dumps (-kd)  
    If specified the dump won't be deleted after successful processing.  

Notice the database location is not specified here, this is always sourced from the alembic.ini file.

For example on Linux, to download and process all dumps, leaving the downloaded dumps afterwards:
```bash
python -m pushshift.pushshift_to_sqlite -dir /mnt/data/openwebtext2 -kd
```

Test run on 2006 only, deleting dumps when done:
```bash
python -m pushshift.pushshift_to_sqlite -s 1,2006 -f 12,2006 -dir /mnt/data/openwebtext2
```

This step uses checkpointing, saving a .dbdone file for each dump once processing is complete. So if you need to stop and come back later you can.

### Extract Unique URLs with Reddit Metadata

This step is performed by "pushshift/generate_urls_from_sqlite.py". The script accepts the following arguments:

--start_period (-s)  
    Month and Year of first URLs. Default: 6,2005  
--finish_period (-f)  
    Month and Year of final URLs. Defaults to current month.  
--output_directory (-dir)  
    Base directory that will contain the urls subdirectory created as part of the process.  
--urls_per_file  
    Maximum number of urls per file. Defaults to 100,000.  

Notice the database location is not specified here, this is always sourced from the alembic.ini file.

For example on Linux, to extract all urls 
```bash
python -m pushshift.generate_urls_from_sqlite -dir /mnt/data/openwebtext2
```

Test run on 2006 only:
```bash
python -m pushshift.generate_urls_from_sqlite -s 1,2006 -f 12,2006 -dir /mnt/data/openwebtext2
```

## Stage 2 - Scraping From Sourced URLs

This stage is performed by "scraping/scrape_urls.py". Note that this is quite a long process, taking us
several weeks.

This program iterates through URL files generated in step 2 above. For each file its hands out the URLs
to a multiprocessing pool for scraping. Once all URLs in the batch are scraped, the successful results are 
archived using a slightly modified version of [lm_dataformat](https://github.com/leogao2/lm_dataformat)
(thanks @bmk). The following metadata fields are saved in the metadata dict offered by lm_dataformat:

title: Web Page Title  
lang: Language detected by Newspaper scraper.  
url: Original URL.  
word_count: Total words outputted by Newspaper.  
elapsed: Scraping time.  
scraper: Always "newspaper".  
domain: Top level domain for the original URL.  
reddit_id: List of submission IDs containing URL - converted from base36.  
subreddit: List of subreddits for the corresponding submissions.  
reddit_score: List of reddit scores for the corresponding submissions.  
reddit_title: List of submissions titles for the corresponding submissions.  
reddit_created_utc: List of submissions created times for the corresponding submissions.

The script accepts the following arguments:

--job_directory (-dir)  
    Base directory containing the urls subdirectory and location where the scrapes subdirectory  will be created.  
--process_count (-procs)  
    Number of worker processes in the pool. Defaults to 60. Don't go above this on Windows.  
--request_timeout (-timeout)  
    Scraping timeout for each URL. Defaults to 30 seconds.  

The program will look for URL files within "job_directory/urls". All scrapes will be stored in "job_directory/scrapes"

For example on Linux, this will scrape using 90 processes and 30 second timeout:
```bash
python -m scraping.scrape_urls -dir /mnt/data/openwebtext2 -procs 90
```

On a dedicated 2012 i7 Linux machine we used between 90 and 120 processes successfully.

We do some limited URL filtering in "scraping/filter.py". This is mainly to speed up the process by avoiding timeouts or files that obviously won't contain text.

NOTE: Once each URL file is scraped, the program saves a ".done" file so you can resume later without rescraping.
      That file contains a count of successfully scraped URLs if you are interested.

## Stage 3 - Filtering scraped documents by minimum total Reddit score

This step is performed by "cleaning/filter_from_reddit_score.py".

This script filters all scrape files "scrapes_*.jsonl.zst" by minimum total Reddit score.
Unlike the original WebText we aggregate scores for all submissions containing a given
URL so the bar is slightly lower in some cases, but in others where a URL went negative
in one submission it will balance out.

The filtered scrapes file will have the original name and path of the scrape file with a 
".minscored" extension.

The script accepts the following arguments:

--scrape_directory (-dir)  
    Directory containing the scrapes. You could use the overall work directory if you want as we use glob.glob to search recursively.  

For example on Linux:
```bash
python -m cleaning.filter_from_reddit_score -dir /mnt/data/openwebtext2/scrapes
```

## Stage 4 - Deduplicate Filtered Documents using MinHashLSH with Cassandra

There are several sub-stages here:

1. Generate minhashes for every document
2. Batch up the minhashes for running parallel dedupe
3. Using MinHashLSH With Cassandra - Generate lists of duplicates
4. Deduplicating our documents using the lists from step 3.

All scripts for this stage can be found within the "cleaning" package.

### Generate Minhashes For Every Document

This step is performed by "cleaning/generate_minhashes.py" and took about 1.5 days
on a 2012 i7 Linux machine.

This script calculates minhashes for all filtered scrape files found using a recursive
search on "\*.minscored".

More explicity, we create a set of 5-grams for each document, and generate document 
level minhashes using 10 hash functions with the excellent datasketch library.

A single file "minhashes.pkl" is created in the scrape directory storing a data
structure in the following format:

[(file_name1, [doc0_minhash, doc1_minhash, ...]), (file_name2, [....]), ....]

The script accepts the following arguments:

--scrape_directory (-dir)  
    Directory containing the minscored scrapes. You could use the overall work directory if you want as we use glob.glob to search recursively.  
--process_count (-procs)  
    Number of worker processes in the pool. Defaults to 4.  

For example on Linux:
```bash
python -m cleaning.generate_minhashes -dir /mnt/data/openwebtext2/scrapes
```

### Slice The Minhashes For Batching

This step is performed by "cleaning/minhash_lsh_batching.py" and splits "minhashes.pkl" into 
approximately the desired number of batches.

The script accepts the following arguments:

--directory (-dir)  
    Directory containing the 'minhashes.pkl' file. Batch files and file name lookup will be saved here.  
--number_of_batches (-batches)  
    Approximate number of batches to split minhashes into.  

The "directory" must contain a 'minhashes.pkl' file created with 'generate_minhashes.py'.

Produces batch files named 'batch0.pkl, batch1.pkl ...'. They contain the following
pickled data structure:
[(file_id, [doc0_minhash, doc1_minhash, ...]), ....]

Produces a file name lookup named 'file_name_lookup.pkl'. Contains the following
pickled data structure:
[file_name1, file_name2, file_name3, ...]


For example on Linux with 16 batches:
```bash
python -m cleaning.minhash_lsh_batching -dir /mnt/data/openwebtext2/scrapes -batches 16
```

### Find Duplicates Using MinHashLSH with Cassandra

This step is performed by "cleaning/minhash_lsh_dedupe.py" and generates a list of detected
duplicates for each located "batch\*.pkl" file found in the batch_directory.

The script accepts the following arguments:

--batch_directory (-dir)  
    Directory containing the "batch\*.pkl" files. "lsh.pkl", duplicate lists and batch checkpoints will be saved here.  
--process_count (-procs)  
    Number of processes in the pool. Defaults to 1.  

See the file for more documentation, but importantly: once you run the script you must keep using 
the same MinHashLSH object pickled to "lsh.pkl" if you want the same basenames for the created Cassandra 
tables.

We are running into issues with loading this MinHashLSH object in multiple processes
so did our run with just a single process. The first run will freeze when trying
to unpickle the MinHashLSH object in the worker process. On running a second time it 
will work.

We save file and document level checkpoints after each query to allow for easy resuming.
Each batch file will have a corresponding "\*duplicates.txt" when done.

For example on Linux with the default 1 process:

```bash
python -m cleaning.minhash_lsh_dedupe -dir /mnt/data/openwebtext2/scrapes
```

### De-Duplicating Using Generated Duplicate Lists

This step is performed by "cleaning/dedupe_from_indexes.py", which accept the following arguments:

--batch_directory (-dir)  
    Directory containing the "\*duplicates.txt" files along with the "file_name_lookup.pkl" created during batch slicing. The "\*final.jsonl.zst" files will be output in their original directories.  

This script builds a list of all duplicates by file_id & document_id, and then iterates
through all ".minscored" files from the filename lookup, creating a new archive for each 
file in the original containing all documents that were not marked as duplicates during 
the previous step.

So for each original file, a "\*final.jsonl.zst" files will be output in the same directory.

For example on Linux:
```bash
python -m cleaning.dedupe_from_indexes -dir /mnt/data/openwebtext2/scrapes
```

## Stage 5 - Packaging The Dataset Releases

### Plug And Play Release

We originally did processing by month, but now just use files containing scrapes for the original
URL files.

Simply tar all the "\*final.jsonl.zst" files.

### Raw Scrapes Release

Similarly just tar all the "scrapes_\*.jsonl.zst" files.

## Stage 6 - Produce Release Stats

If you move the files from each release into their own subdirectory, you can run the "data_analysis/final_stats.py"
to get total document count and text size for all "jsonl.zst" files in each directory:

For example on Linux:
```bash
python -m data_analysis.final_stats -dir /mnt/data/openwebtext2/final
python -m data_analysis.final_stats -dir /mnt/data/openwebtext2/raw_release
```

