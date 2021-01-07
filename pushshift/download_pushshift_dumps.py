"""
This module is responsible for downloading the PushShift submission dump
files, and contains the following functions:

Functions
---------
get_url_content_length(url)
    Attempts to retrieve the Content-Length header when performing
    a get request on the provided 'url'. Returns the number of bytes
    if available, or None.

build_file_list(start_date, end_date):
    Builds a list of PushShift submission dump files located at 
    "https://files.pushshift.io/reddit/submissions" within the desired date
    range.

get_sha256sums
    Downloads the sha256sum file for the PushShift submission dumps from
    "https://files.pushshift.io/reddit/submissions/sha256sums.txt". Builds
    and returns a dictionary with file name as key and sha256 sum as value.
"""

from dateutil.relativedelta import *
import math

import requests

import logging
logger = logging.getLogger(__name__)

million = math.pow(10, 6)
possible_archive_formats = ["zst", "xz", "bz2"]

def get_url_content_length(url):
    response = requests.head(url)
    response.raise_for_status()

    if "Content-Length" in response.headers:
        return int(response.headers['Content-length'])
    else:
        return None

def build_file_list(start_date, end_date):
    base_url = "https://files.pushshift.io/reddit/submissions"
    url_list = []
    date = start_date
    current_year = None
    while date <= end_date:
        year = date.strftime("%Y")
        month = date.strftime("%m")

        if year != current_year:
            current_year = year
            logger.info(f"Scanning Year {current_year}")

        if year < "2011":
            url = f"{base_url}/RS_v2_{year}-{month}.xz"
            url_list.append(url)
        else:
            for extension in possible_archive_formats:
                url = f"{base_url}/RS_{year}-{month}.{extension}"
                try:
                    get_url_content_length(url) # If this fails there's no file
                    url_list.append(url)
                    break
                except:
                    pass

        date = date + relativedelta(months=+1)

    return url_list

def get_sha256sums():
    sha256sum_url = "https://files.pushshift.io/reddit/submissions/sha256sums.txt"

    sha256sum_lookup = {}
    with requests.get(sha256sum_url) as response:
        response.raise_for_status()
        for line in response.text.splitlines():
            if line.strip():
                sha256sum, file_name = tuple(line.strip().split("  "))
                sha256sum_lookup[file_name] = sha256sum

    return sha256sum_lookup