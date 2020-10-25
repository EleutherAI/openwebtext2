# Dataset Replication

This area of the documentation provides instructions for building the full dataset from scratch. If you just want the dataset, please see [Welcome](/).

<a href="https://www.reddit.com/r/pushshift/comments/bcxguf/new_to_pushshift_read_this_faq" target="_blank">PushShift</a> provides dumps of all reddit posts and submissions, however they are normally a few months behind. While this would be problematic for certain use cases, we didn't require up to the minute data for training GPTNeo. In the future we may look into getting recent data either by scraping Reddit directly or using one of the existing APIs. 

## Pipeline Overview

At a high level the pipeline works as follows:

1. Download and process the PushShift submission dumps to extract unique URLs & Metadata.
2. Scrape the URLs using <a href="https://newspaper.readthedocs.io/en/latest/" target="_blank">Newspaper3k</a>, saving both text and metadata with <a href="https://github.com/leogao2/lm_dataformat" target="_blank">lm_dataformat</a>.
5. Filter the scraped documents by minimum Reddit score 3.
4. Perform fuzzy deduplication using <a href="http://ekzhu.com/datasketch/lsh.html" target="_blank">MinHashLSH</a>.
5. Package up the various dataset releases.
6. Produce some useful size stats for the releases.

## Environment Setup
We tested everything on Ubuntu 18/20 & Windows 10 with miniconda. You could use virtualenv, venv or even the global python environment if you wish.

### Miniconda Install For Linux

Follow the below steps, or read the conda instructions:<br/>
<a href="https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html" target="_blank">https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html</a>

```bash
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
sha256 Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
```
Select yes on the init step.

Restart your shell to refresh the path.

### Create and activate conda environment

Environments are saved in a central store on the local disk, no need to create folders like with venv.
```
conda create --name pushshift python=3.8
conda activate pushshift
```

### Install Repo and Requirements
```bash
git clone https://github.com/EleutherAI/openwebtext2.git
cd openwebtext2
pip install -r requirements.txt
```

### General Recommendations 

Use the screen command if running a remote terminal, many scripts below take a long time to run and while they often support resuming it's better not to rely on it.

Create a working directory on a drive with at least 500gb of space.

## Stage 1 - Processing PushShift Submission Dumps

This stage consists of the following steps:

1. Sqlite database setup.
2. Download and verify the PushShift submission dumps, extracting and storing urls and metadata
from relevant submissions into the sqlite database. Performed by "pushshift/pushshift_to_sqlite.py".
3. Query the populated sqlite database and build a list of URLs with metadata for all related submissions.
Performed by "pushshift/generate_urls_from_sqlite.py"

All scripts for this stage can be found within the "pushshift" package.

### Sqlite Database Setup
We use alembic to manage the sqlite database in case you want to add extra indexes or fields easily later.

Inside the cloned repo:
```bash
cp alembic.ini.template alembic.ini
```
Modify the following line within the alembic.ini file. For example on windows:
```python
sqlalchemy.url = sqlite:///e:/Eleuther_AI/openwebtext2/submissions.sqlite
```
Or on Linux (notice the extra forward slash before "mnt" to indicate system root):
```python
sqlalchemy.url = sqlite:////mnt/data/openwebtext2/submissions.sqlite
```

### Insert PushShift Submission Data Into Sqlite

This step is performed by *pushshift/pushshift_to_sqlite.py*.

| Script Argument      | Description |
| -----------: | ----------- |
| `--start_period (-s)` | Month and Year of first pushshift dump. Default: 6,2005.     |
| `--finish_period (-f)` | Month and Year of final pushshift dump. Defaults to current month. |
| `--output_directory (-dir)` | Will contain the dumps subdirectory created as part of the process.    | 
| `--keep_dumps (-kd)` | If specified the dumps won't be deleted after successful processing.     | 

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

This step is performed by *pushshift/generate_urls_from_sqlite.py*.

| Script Argument      | Description |
| -----------: | ----------- |
| `--start_period (-s)` | Month and Year of first URLs. Defaults to None (query all URLs).     |
| `--finish_period (-f)` | Month and Year of final URLs. Defaults to None (query all URLs). |
| `--output_directory (-dir)` | Will contain the urls subdirectory created as part of the process.    | 
| `--urls_per_file` | Maximum number of urls per file. Defaults to 100,000.    | 

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

This stage is performed by *scraping/scrape_urls.py* and took several weeks compute time. To decrease this you can run on multiple servers by passing out the URL files.

| Script Argument      | Description |
| -----------: | ----------- |
| `--job_directory (-dir)` | Base directory containing the urls subdirectory and location where the scrapes subdirectory will be created.       |
| `--process_count (-procs)` | Number of worker processes in the pool. Defaults to 60. Don't go above this on Windows. |
| `--request_timeout (-timeout)` | Scraping timeout for each URL. Defaults to 30 seconds.  | 

The script iterates through URL files generated in step 2 above. For each file its hands out the URLs
to a multiprocessing pool for scraping. Once all URLs in the batch are scraped, the successful results are 
archived using a slightly modified version of <a href="https://github.com/leogao2/lm_dataformat" target="_blank">lm_dataformat</a>. For each document (URL), the following metadata fields are saved in the metadata dict offered by lm_dataformat:

| Meta Field      | Description |
| -----------: | ----------- |
| title      | Web Page Title.       |
| lang   | Language detected by Newspaper scraper.        |
| url   | Original URL.       |
| word_count   | Total words outputted by Newspaper.         |
| elapsed   |  Scraping time.       |
| scraper   |  Always "newspaper".        |
| domain   |   Top level domain for the original URL.      |
| reddit_id   |   List of submission IDs containing URL - converted from base36.        |
| subreddit   |   List of subreddits for the corresponding submissions.       |
| reddit_score   | List of reddit scores for the corresponding submissions.         |
| reddit_title   |   List of submissions titles for the corresponding submissions.       |
| reddit_created_utc      | List of submissions created times for the corresponding submissions.      |

The program will look for URL files within "job_directory/urls". All scrapes will be stored in "job_directory/scrapes"

For example on Linux, this will scrape using 90 processes and 30 second timeout:
```bash
python -m scraping.scrape_urls -dir /mnt/data/openwebtext2 -procs 90
```

On a dedicated 2012 i7 Linux machine we used between 90 and 120 processes successfully.

We do some limited URL filtering in *scraping/filter.py*. This is mainly to speed up the process by avoiding timeouts or files that obviously won't contain text.

Once each URL file is scraped, the program saves a ".done" file so you can resume later without rescraping. That file contains a count of successfully scraped URLs if you are interested.

## Stage 3 - Filtering scraped documents by minimum total Reddit score

This stage is performed by *cleaning/filter_from_reddit_score.py*.

| Script Argument      | Description |
| -----------: | ----------- |
| `--scrape_directory (-dir)` | Directory containing the scrapes. You could use the overall work directory if you want as we use glob.glob to search recursively.         |

The script filters all scrape files "scrapes_*.jsonl.zst" by minimum total Reddit score.
Unlike the original WebText we aggregate scores for all submissions containing a given
URL so the bar is slightly lower in some cases, but in others where a URL went negative
in some submission it will balance out.

The filtered scrapes file will have the original name and path of the scrape file with a 
".minscored" extension.

For example on Linux:
```bash
python -m cleaning.filter_from_reddit_score -dir /mnt/data/openwebtext2/scrapes
```

## Stage 4 - Deduplicate Filtered Documents using MinHashLSH with Cassandra

There are several sub-stages here:

1. Setup Cassandra
2. Generate minhashes for every document
3. Batch up the minhashes for running parallel dedupe
4. Using MinHashLSH With Cassandra - Generate lists of duplicates
5. Deduplicating our documents using the lists from step 3.

All scripts for this stage can be found within the "cleaning" package.

### Setup Cassandra

We used a local Cassandra install, simplifying the setup process. Some good cassandra guides:

<a href="https://www.tecmint.com/install-apache-cassandra-on-ubuntu/" target="_blank">Installation Guide For Ubuntu 20</a><br/>
<a href="https://towardsdatascience.com/getting-started-with-apache-cassandra-and-python-81e00ccf17c9" target="_blank">Introduction To Cassandra + Connecting With Python API</a>

Summarized Quick Install For Ubuntu 20:
```bash
sudo apt install openjdk-8-jdk
sudo apt install apt-transport-https
wget -q -O - https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -
sudo sh -c 'echo "deb http://www.apache.org/dist/cassandra/debian 311x main" > /etc/apt/sources.list.d/cassandra.list'
sudo apt update
sudo apt install cassandra
sudo systemctl status cassandra
```

To test your installation was successful, run the cqlsh CLI:
```bash
cqlsh
```

Once inside:
```describe keyspaces```

If you want multiple nodes or remote connection you need to set the following in your /etc/cassandra/cassandra.yaml:

seeds: "your_server_external_ip, other nodes in cluster"<br/>
listen_address: your_server_external_ip<br/>
start_rpc: true<br/>
rpc_address: 0.0.0.0 (this will bind to same address as listen_address)

For some reason they recommend not to make this available on the internet despite supporting various forms of authentication. So either use a tunnel or fancy networking to get around this.

### Generate Minhashes For Every Document

This step is performed by *cleaning/generate_minhashes.py* and took about 1.5 days
on a 2012 i7 Linux machine.

| Script Argument      | Description |
| -----------: | ----------- |
| `scrape_directory (-dir)` | Directory containing the minscored scrapes. You could use the overall work directory if you want as we use glob.glob to search recursively.           |
| `process_count (-procs)` | Number of worker processes in the pool. Defaults to 4.  |

This script calculates minhashes for all filtered scrape files found using a recursive
search on "\*.minscored".

More explicity, we create a set of 5-grams for each document, and generate document 
level minhashes using 10 hash functions with the excellent datasketch library.

A single file "minhashes.pkl" is created in the scrape directory storing a data
structure in the following format:

```python
[(file_name1, [doc0_minhash, doc1_minhash, ...]), (file_name2, [....]), ....]
```

For example on Linux:
```bash
python -m cleaning.generate_minhashes -dir /mnt/data/openwebtext2/scrapes
```

### Slice The Minhashes For Batching

This step is performed by *cleaning/minhash_lsh_batching.py*.

| Script Argument      | Description |
| -----------: | ----------- |
| `directory (-dir) ` | Directory containing the 'minhashes.pkl' file. Batch files and file name lookup will be saved here.             |
| `number_of_batches (-batches)  ` | Approximate number of batches to split minhashes into.   |

The "directory" must contain a 'minhashes.pkl' file created with *cleaning/generate_minhashes.py*.

This splits "minhashes.pkl" into approximately the desired number of batches, producing batch files named 'batch0.pkl, batch1.pkl, etc'. They contain the following pickled data structure:
```python
[(file_id, [doc0_minhash, doc1_minhash, ...]), ....]
```

It also creates a file name lookup named 'file_name_lookup.pkl' containing the following pickled datastructure:
```python
[file_name1, file_name2, file_name3, ...]
```

For example on Linux with 16 batches:
```bash
python -m cleaning.minhash_lsh_batching -dir /mnt/data/openwebtext2/scrapes -batches 16
```

### Find Duplicates Using MinHashLSH with Cassandra

This step is performed by *cleaning/minhash_lsh_dedupe.py*.

| Script Argument      | Description |
| -----------: | ----------- |
| `batch_directory (-dir)` | Directory containing the "batch\*.pkl" files. Duplicate lists and batch checkpoints will be saved here.             |
| `process_count (-procs)` | Number of processes in the pool. Defaults to 4. |

The script generates a list of detected duplicates for files/documents located in the various "batch\*.pkl" files.

We save file and document level checkpoints after each query to allow for easy resuming.
Each batch file will have a corresponding "\*duplicates.txt" when done.

For example on Linux with the default 4 processes:

```bash
python -m cleaning.minhash_lsh_dedupe -dir /mnt/data/openwebtext2/scrapes
```

### De-Duplicating Using Generated Duplicate Lists

This step is performed by *cleaning/dedupe_from_indexes.py*.

| Script Argument      | Description |
| -----------: | ----------- |
| `batch_directory (-dir)` | Directory containing the "\*duplicates.txt" files along with the "file_name_lookup.pkl" created during batch slicing. The "\*final.jsonl.zst" files will be output in their original directories.               |

This script builds a list of all duplicates by file_id & document_id, and then iterates
through all ".minscored" files from the filename lookup, creating a new archive for each 
file in the original containing all documents that were not marked as duplicates during 
the previous step.

For each original file, a "\*final.jsonl.zst" files will be output in the same directory.

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