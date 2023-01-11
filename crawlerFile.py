'''
This is the crawler script that is called by main script
For it to run successfully Scrapy 2.7.1 needs to be installed from https://docs.scrapy.org/en/latest/intro/install.html
'''

import scrapy,os
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from urllib.parse import urlparse
from scrapy.http import Request

class CrawlerConcordia(CrawlSpider):

    name = 'ProjectCrawler'
    allowed_domains = ['www.concordia.ca']
    start_urls = ['https://www.concordia.ca/ginacody']

    rules = (
            Rule(LinkExtractor(allow=r'https://www.concordia.ca/ginacody*'), callback='parse', follow=True),
            )  
   
   # set location of file download
    cwd = os.getcwd()
    dirName = "HTMLFiles\\"
    global absdirPath
    absdirPath = os.path.join(cwd,dirName)
    if not os.path.isdir(absdirPath):
        os.mkdir(absdirPath)
    global pfile 
    pfile = cwd + "\parsedfiles.txt"

    def parse(self, response):
        # get the indexing status of the page
        windex = response.xpath("//meta[@name='robots'][1]/@content").extract()[0].split(',')[0]
       
        if windex=='index':
            with open(pfile, 'a+') as pf:
                pf.write(response.url)
                pf.write("\n")
            pf.close()

            filename = response.url.split(".html")
            filename = filename[0].split("/")[-1] + ".html"
            print(filename)
            # # if "?" in filename:
            # #     filename = filename.split("?")[0]
            filefullpath = absdirPath + filename
            with open(filefullpath, 'wb') as f:
                f.write(response.body)
            f.close()