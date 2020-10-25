# Welcome!

OpenWebText2 is an enhanced version of the original OpenWebTextCorpus covering all Reddit submissions from 2005 up until April 2020, with further months becoming available after the corresponding PushShift dump files are released.

In case you haven't heard of WebText, the core principle is extracting URLs from reddit submissions, scraping the URLs, then performing filtering & deduplication. See [Background](/Background) for more information.

## Download the Data

### Plug and Play Version
This version has already been cleaned for you:

- Deduplicated by URL
- Filtered by minimum combined reddit score 3
- Deduplicated at document level with MinHashLSH.

Contains 17,103,059 documents for a total of 65.86gb uncompressed text.  
openwebtext2_pnp.tar (28gb compressed including text and metadata) **currently uploading**

### Raw Scrapes
Only deduplicated by URL.

Contains 69,547,149 documents for a total of 193.89gb uncompressed text.  
openwebtext2_raw.tar (79gb compressed including text and metadata) **currently uploading**

## Using The Data

The data is stored using <a href="https://github.com/leogao2/lm_dataformat" target="_blank">lm_dataformat</a>. We use a slightly modified version to allow file peeking for tqdm progress bars: <a href="https://github.com/EleutherAI/openwebtext2/blob/master/utils/archiver.py" target="_blank">utils/archiver.py</a>. Be sure to call *read_jsonl* with `get_meta=True` as both versions contain useful metadata for each document, including several original Reddit fields.

```python
import glob
import os
import math

import tqdm

from utils.archiver import Reader

document_count = 0
total_text_size = 0
dataset_directory = "PATH_TO_FILES"
files = glob.glob(os.path.join(dataset_directory, "*jsonl.zst"))
for file_path in tqdm.tqdm(files, dynamic_ncols=True):
    reader = Reader()
    for document, metadata in reader.read_jsonl(file_path, get_meta=True):
        document_count += 1
        total_text_size += len(document)

billion = math.pow(10, 9)
print(f"Total Document Count: {document_count:,}")
print(f"Total Uncompressed Text Size: {(total_text_size / billion):.2f} GB")
```

Alternatively checkout <a href="https://github.com/EleutherAI/The-Pile/" target="_blank">The-Pile</a>, which acts as an aggregator/dataloader for multiple text datasets. It allows you to configure your total data size requirement, along with the desired weighting for each subset. Once configured, you get a randomized stream of documents, allowing easy feeding to your language model.

