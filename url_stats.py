import json
import tldextract
import os

# Stuffing around looking at URLs for extensions and domains
def analysis(url_file_name, stats_file_name):  
    urls = json.load(open(url_file_name,"r"))

    extensions = set()

    baseDomains = set()
    domains = set()

    # i = 0
    for url in urls:
        # i += 1
        # if i > 100:
            # break
        url = url.strip("/")        
        # print(url)
        urlFile = url.split("/")[-1]
        # print(urlFile)
        lastBitNoQuery = urlFile.split("?")[0]
        # print(lastBitNoQuery)
        if "." not in lastBitNoQuery:
            # print("No Extension")
            extensions.add("No Extension")
        else: 
            extension = lastBitNoQuery.split(".")[-1]
            # print(extension)        
            # if extension == "054569v1":
            extensions.add(extension)

        # print(url)
        ext = tldextract.extract(url)
        # print(ext)
        domain = '.'.join([x for x in ext if x])
        domains.add(domain)
        # print(domain)
        baseDomain = '.'.join(ext[-2:])
        baseDomains.add(baseDomain)
        # print(baseDomain)

    # print(extensions)

    stats = json.load(open(stats_file_name,"r"))

    print("Post Count: ", stats["postCount"])
    print("Relevant Post Count: ", stats["relevantPostCount"])

    # print(domains)
    # print(baseDomains)

if __name__ == '__main__':
    base_directory = "E:/Eleuther_AI/webtext2/dumps/output"
    url_file_name = os.path.join()
    stats_file_name= "E:/Eleuther_AI/webtext2/dumps/RS_2020-04.stats"
    analysis(url_file_name, stats_file_name)