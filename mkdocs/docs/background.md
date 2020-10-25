# WebText Background

<a href="https://openai.com/" target="_blank">OpenAI</a> required around 40gb of high quality text corpus for training <a href="https://openai.com/blog/better-language-models/" target="_blank">GPT2</a>. While Common Crawl provides the scale necessary for modern language models, the quality is unreliable. Manual curation of Common Crawl is always an option, albeit an expensive one. Thankfully Reddit provides decentralized curation by design, and this became the key innovation for the WebText dataset.

The generation of WebText can be summarized as:

1. Scrape URLs from all Reddit submissions up to December 2017 with 3 or higher score.
2. Deduplicate scraped content based on URL
3. Exclude wikipedia - OpenAI already had a separate Wikipedia dataset
4. Deduplicate remaining content using undisclosed "heuristic based cleaning". This includes removal of non-english web pages.

Neither the resulting corpus or generation source code was made public, inspiring Aaron Gokaslan and Vanya Cohen to create the <a href="https://skylion007.github.io/OpenWebTextCorpus/" target="_blank">OpenWebTextCorpus</a>.

OpenWebTextCorpus is an open source reproduction of WebText, reifying the "heuristic based cleaning" stage with fuzzy deduplication and enforcing a minimum token length. For content based de-duplication they used local-sensitivity-hashing (LSH) with minhash on sets of 5-grams at the document level. Documents were then tokenized and any with less then 128 tokens were removed. After all processing there remained 40GB of text across 8,013,769 documents. 

The original code for OpenWebTextCorpus unavailable at this time, but there are several popular repositories that cover the pipeline to various degrees.

## OpenWebText2 Motivation

Our primary goals for the corpus are:

1. More data! Coverage of the original OpenWebTextCorpus ended at December 2017.
2. Include all languages, providing metadata for easy filtering
3. Provide several versions of the generated corpus for differing user requirements. Both versions will be broken up by month and frozen, with future months available once PushShift submission dumps become available.
    * Raw version containing all scraped pages with associated Reddit submission metadata
    * Plug and play version based on submissions of minimum 3 score with content based fuzzy de-duplication
4. Provide full source code for all stages of the pipeline including deduplication.

We decided on a rewrite taking inspiration from both <a href="https://github.com/yet-another-account/openwebtext/" target="_blank">1</a> and <a href="https://github.com/jcpeterson/openwebtext" target="_blank">2</a>.