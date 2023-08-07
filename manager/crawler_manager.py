import threading
from torcrawler import Crawler
from config.CrawlerInstance import crawlerinstance
                
class CrawlerManager:
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(CrawlerManager, cls).__new__(cls)
        return cls._instance

        
    def __init__(self):
        print("initiating crawler manager")
        if not hasattr(self, 'crawlers'):
            self.crawlers = {}
            
    def create_crawler(self, crawler_doc: crawlerinstance):
        if crawler_doc.url not in self.crawlers:
            new_crawler = Crawler(crawlerId=crawler_doc.id)
            self.crawlers[crawler_doc.id] = new_crawler
            print("created crawler, now length is" , self.crawlers.__len__())
            new_crawler.crawl(crawler_doc.url, limit=crawler_doc.limit)
        else:
            pass

    def stop_crawler(self, crawler_doc: crawlerinstance):
        if crawler_doc.id in self.crawlers:
            crawler: Crawler = self.crawlers[crawler_doc.id]
            crawler.stop_threads()
            del self.crawlers[crawler_doc.id]
            print("length after deleting url is " , self.crawlers.__len__())