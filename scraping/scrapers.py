# Code taken in large part from https://github.com/jcpeterson/openwebtext

import time
import unicodedata

import bs4
import newspaper

from lxml.html.clean import Cleaner
from htmlmin import minify
from scraping.filter import should_exclude


def find_and_filter_tag(tag, soup):
    """tag specific filter logic"""

    candidates = soup.find_all(tag)
    candidates = [
        unicodedata.normalize("NFKD", x.string)
        for x in candidates
        if x.string is not None
    ]

    if tag == "p":
        candidates = [y.strip() for y in candidates if len(y.split(" ")) >= 4]
        count = sum(len(y.split(" ")) for y in candidates)
    else:
        raise NotImplementedError

    return (candidates, count)


def raw_scraper(url, memoize):
    t1 = time.time()
    if should_exclude(url):
        # heuristic to make downloading faster
        return None, {
            "url": url,
            "scraper": "raw",
        }

    try:
        cleaner = Cleaner()
        cleaner.javascript = True
        cleaner.style = True
        article = newspaper.Article(url, fetch_images=False, memoize_articles=memoize)
        article.download()
        html = minify(article.html)
        html = cleaner.clean_html(html)
        article.parse()
    except:
        return None, {
            "url": url,
            "scraper": "raw",
        }
    if article.text == "":
        return None, {
            "url": url,
            "scraper": "raw",
        }

    metadata = {"url": url, "elapsed": time.time() - t1, "scraper": "raw"}
    return html, metadata


def newspaper_scraper(url, memoize, request_timeout):
    t1 = time.time()

    if should_exclude(url):
        # heuristic to make downloading faster
        return None, {
            "url": url,
            "scraper": "newspaper",
        }, False

    try:
        article = newspaper.Article(url, fetch_images=False, memoize_articles=memoize, 
                                    request_timeout=request_timeout)
        article.download()
        article.parse()
    except Exception as ex:
        return None, ex, False

        #print(article.__dict__.keys())                
        #dict_keys(['config', 'extractor', 'source_url', 'url', 'title', 'top_img', 'top_image', 'meta_img', 'imgs', 'images', 
        #           'movies', 'text', 'keywords', 'meta_keywords', 'tags', 'authors', 'publish_date', 'summary', 'html', 'article_html', 
        #           'is_parsed', 'download_state', 'download_exception_msg', 'meta_description', 'meta_lang', 'meta_favicon', 'meta_data', 
        #           'canonical_link', 'top_node', 'clean_top_node', 'doc', 'clean_doc', 'additional_data', 'link_hash'])
        
        #print(article.title)
        # print(article.meta_lang)
        # if article.meta_lang != "en" and article.meta_lang:
        #     print(article.text)

    text = article.text
    count = len(text.split())

    metadata = {
        "title": article.title,
        "lang": article.meta_lang,
        "url": url,
        "word_count": count,
        "elapsed": time.time() - t1,
        "scraper": "newspaper",
    }
    return text, metadata, True

def bs4_scraper(url, memoize):
    t1 = time.time()
    if should_exclude(url):
        # heuristic to make downloading faster
        return None, {
            "url": url,
            "scraper": "bs4",
        }

    try:
        article = newspaper.Article(url, fetch_images=False, memoize_articles=memoize)
        article.download()
        html = article.html
        soup = bs4.BeautifulSoup(html, "lxml")
        text, count = find_and_filter_tag("p", soup)
        # DDB: keep text as a single string for consistency with
        # newspaper_scraper
        text = " ".join(text)
    except:
        return None, {
            "url": url,
            "scraper": "bs4",
        }

    metadata = {
        "url": url,
        "word_count": count,
        "elapsed": time.time() - t1,
        "scraper": "bs4",
    }
    return text, metadata
