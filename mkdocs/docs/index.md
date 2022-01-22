# Welcome!

OpenWebText2 is an enhanced version of the original OpenWebTextCorpus covering all Reddit submissions from 2005 up until April 2020, with further months becoming available after the corresponding PushShift dump files are released.

In case you haven't heard of WebText, the core principle is extracting URLs from reddit submissions, scraping the URLs, then performing filtering & deduplication. See [Background](background) for more information.

<hr />

## Download Plug and Play Version
This version has already been cleaned for you:

- Deduplicated by URL
- Filtered by minimum combined reddit score 3
- Deduplicated at document level with MinHashLSH.

**Stats**<br/>
17,103,059 documents<br/>
65.86 GB uncompressed text<br/>
28 GB compressed including text and metadata


<a href="https://mystic.the-eye.eu/public/AI/pile_preliminary_components/openwebtext2.jsonl.zst.tar">
<button type="button" class="btn btn-outline-primary download-button">
    Download
    <svg width="1em" height="1em" viewBox="0 0 16 16" class="bi bi-download" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
        <path fill-rule="evenodd" d="M.5 9.9a.5.5 0 0 1 .5.5v2.5a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-2.5a.5.5 0 0 1 1 0v2.5a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2v-2.5a.5.5 0 0 1 .5-.5z"></path>
        <path fill-rule="evenodd" d="M7.646 11.854a.5.5 0 0 0 .708 0l3-3a.5.5 0 0 0-.708-.708L8.5 10.293V1.5a.5.5 0 0 0-1 0v8.793L5.354 8.146a.5.5 0 1 0-.708.708l3 3z"></path>
    </svg>
</button>
</a>

<hr />

## Download Raw Scrapes Version
Only deduplicated by URL.

**Stats**<br/>
69,547,149 documents<br/>
193.89gb uncompressed text.<br/>
79gb compressed including text and metadata

<a href="https://eaidata.bmk.sh/data/openwebtext2_raw.tar">
<button type="button" class="btn btn-outline-primary download-button">
    Download
    <svg width="1em" height="1em" viewBox="0 0 16 16" class="bi bi-download" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
        <path fill-rule="evenodd" d="M.5 9.9a.5.5 0 0 1 .5.5v2.5a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-2.5a.5.5 0 0 1 1 0v2.5a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2v-2.5a.5.5 0 0 1 .5-.5z"></path>
        <path fill-rule="evenodd" d="M7.646 11.854a.5.5 0 0 0 .708 0l3-3a.5.5 0 0 0-.708-.708L8.5 10.293V1.5a.5.5 0 0 0-1 0v8.793L5.354 8.146a.5.5 0 1 0-.708.708l3 3z"></path>
    </svg>
</button>
</a>

<hr />

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

## Cite as

<pre style="white-space: pre-wrap;">
@article{pile,
    title={The {P}ile: An 800GB Dataset of Diverse Text for Language Modeling},
    author={Gao, Leo and Biderman, Stella and Black, Sid and Golding, Laurence and Hoppe, Travis and Foster, Charles and Phang, Jason and He, Horace and Thite, Anish and Nabeshima, Noa and Presser, Shawn and Leahy, Connor},
    journal={arXiv preprint arXiv:2101.00027},
    year={2020}
}
</pre>
