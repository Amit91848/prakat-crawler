# import requests
# from stem import Signal
# from stem.control import Controller
# import threading
# from queue import Queue
# import random
# from time import sleep
# from requests.exceptions import HTTPError, ConnectTimeout
# from urllib3.exceptions import MaxRetryError, ConnectTimeoutError
# from bs4 import BeautifulSoup
# from urllib.parse import urlparse, urlunparse
# import signal, pickle, os
# import json
# from threading import Lock, Thread

# PASSWORD = 'ligmaballs'
# SOCKS_PORT = 9051
# MAX_RETRIES = 3
# RETRY_DELAY = 5
# THREAD_DELAY = 1
# LIMIT = 1000
# THREADS = 10
# JSON_FILE = 'data.json'

# class SingletonMeta(type):
#     _instances = {}
#     _lock: Lock = Lock()
    
#     def __call__(cls, *args, **kwargs):
#         with cls._lock:
#             if cls not in cls._instances:
#                 instance = super().__call__(*args, *kwargs)
#                 cls._instances[cls] = instance

#         return cls._instances[cls]


# class CrawlerThread(threading.Thread):
    
#     def __init__(self, target=None, args=(), kwargs=None):
#         super().__init__()
#         self.target = target
#         self.args = args
#         self.kwargs = kwargs if kwargs is not None else {}
#         self.stop_signal = False

#     def run(self):
#         if self.target:
#             self.target(self, *self.args, **self.kwargs)
#         print('Thread stopping')

#     def stop_thread(self):
#         self.stop_signal = True
        

# # class Crawler(metaclass=SingletonMeta): 
# class Crawler():

#     def __init__(self):
#         self.counter = 0
#         self.limit = LIMIT
#         self.q = Queue()
#         self.crawled_links = []
#         self.thread_pool = []
#         self.lock = threading.Lock()

#         self.controller = Controller.from_port(port=SOCKS_PORT)
#         self.controller.authenticate(password=PASSWORD)

#         print(f"Tor is running version {self.controller.get_version()}\n")

#         self.proxies = {
#             'http': 'socks5h://127.0.0.1:9050',
#             'https': 'socks5h://127.0.0.1:9050'
#         }

#         with open('user-agents.txt', 'r') as f:
#             uals = f.read().splitlines()
        
#         self.user_agents = uals
	
#         # self.update_crawled()

#         # if 'queue.pkl' in os.listdir():
#         #     with open('queue.pkl', 'rb') as f:
#         #         queue_list = pickle.load(f)
#         #     print('QUEUE PICKLE FOUND... POPULATING QUEUE FROM STORAGE...\n')
#         #     for element in queue_list:
#         #         self.q.put(element)

        

#     def update_crawled(self):
#         with open('data.json','r') as f:
#             json_list = self.get_json_lines(f)
#         for document_dict in json_list:
#             if 'url' in document_dict.keys():
#                 self.crawled_links.append(document_dict['url'])


#     def get_json_lines(self, file_obj):
#         json_list = []
#         for line in file_obj:
#             json_line = json.loads(line.strip())
#             json_list.append(json_line)
#         return json_list

#     def new_identity(self):
#         self.controller.signal(Signal.NEWNYM)
#         sleep(self.controller.get_newnym_wait())
#         print('NEW IDENTITY\n')

#     def write_to_json(self, data, filename):
#         if not(filename in os.listdir()):
#             with open(filename, 'w') as f:
#                 f.write(json.dumps(data)+'\n')
#         else: 
#             with open(filename, 'a') as f:
#                 f.write(json.dumps(data)+'\n')


#     def request_onion(self, url, retries=MAX_RETRIES, delay=RETRY_DELAY):
#         newid_flag = 0
#         for _ in range(retries):    
#             ua = random.choice(self.user_agents)
#             headers = {'User-Agent':ua} 
#             try:
#                 response = requests.get(url,proxies=self.proxies, headers=headers)
#                 response.cookies.clear()
#                 # print(f'REQUESTED {url} : {response.status_code}')
#                 if response.ok:
#                     return response
#                 else:
#                     response.raise_for_status()
#             except (HTTPError, MaxRetryError, ConnectTimeoutError, ConnectTimeout) as e:
#                 # print(f'ERROR with {url} : {e}\nERROR Type: {type(e)}\n\n')
#                 if newid_flag:
#                     break
#                 self.new_identity()
#                 newid_flag = 1
#             except Exception as e:
#                 # print(f'ERROR with {url} : {type(e)}\n')
#                 break
#             sleep(delay)

#     def process_link(self, link, base_netloc, base_scheme):
#         linkparse = urlparse(link)
#         scheme = linkparse.scheme
#         netloc = linkparse.netloc
#         path = linkparse.path
#         params = linkparse.params
#         query = linkparse.query
#         fragment = ''

#         if not bool(path):
#             path = '/'
#         elif path == '/':
#             pass
#         elif path.endswith('/'):
#             path = path[:-1]

#         if not bool(scheme):
#             scheme = base_scheme

#         if not bool(netloc):
#             netloc = base_netloc

#         if not netloc.endswith('.onion'):
#             scheme = 'https'
            
#         return urlunparse((scheme, netloc, path, params, query, fragment))
   
#     def extract_links(self, onion_resp, url):
#         parse_dict = urlparse(url)
#         base_netloc = parse_dict.netloc
#         base_scheme = parse_dict.scheme
#         soup = BeautifulSoup(onion_resp.text, 'lxml')
        
#         anchor_list = soup.find_all("a")
#         link_list = []
#         for anchor in anchor_list:
#             try:
#                 processed_link = self.process_link(anchor["href"], base_netloc, base_scheme)
#                 linkparse = urlparse(processed_link)
#                 if not linkparse.netloc.endswith('.onion'):
#                     continue
#                 if processed_link != '':
#                     link_list.append(processed_link)
#             except:
#                 continue
#         return link_list
    
#     def get_data(self, onion_resp, url):
#         onion_data = {}
#         soup = BeautifulSoup(onion_resp.text, 'lxml')
#         onion_data['url'] = url
#         if soup.title:
#             onion_data['title'] = soup.title.text
#         else: 
#             onion_data['title'] = ''
#         onion_data['body'] = soup.body.get_text()
#         onion_data['response-headers'] = dict(onion_resp.headers)
#         return onion_data
    
#     def populate_queue(self, url):
#         try:
#             response = self.request_onion(url)
#         except Exception as e:
#             # print(f'Connection with {url} FAILED...\n')
#             print(f'ERROR: {e}\t{type(e)}')
#         if response is None:
#             print(f'ERROR: LOCATION RETURNED NONE\n')
        
#         links = self.extract_links(response, url)
#         for link in links:
#             if not (link in self.crawled_links):
#                 self.q.put(link)
#         print(f'QUEUE POPULATED WITH LINKS FROM {url}\n')
         
#     def thread_crawl(self, thread):
#         while (self.q.empty() == False) and (self.counter < self.limit):
#             if thread.stop_signal:
#                 break
#             url = self.q.get()
#             with self.lock:                   
#                 if url in self.crawled_links:
#                     continue
#                 self.crawled_links.append(url)
#             try:
#                 response = self.request_onion(url)
#             except Exception as e:
#                 print(f'Connection with {url} FAILED...\n')
#                 print(f'ERROR: {e}\t{type(e)}')
#                 continue
#             if response is None:
#                 print(f'ERROR: LOCATION RETURNED NONE\n')
#                 continue

#             links = self.extract_links(response, url)
            
#             with self.lock:
#                 for link in links:
#                     if not (link in self.crawled_links):
#                         self.q.put(link)
            
#             # EXTRACTING INFORMATION FROM URL RESPONSE  
#             try:          
#                 page_data = self.get_data(response, url)
#             except Exception as e:
#                 print(f'ERROR extracting data: {e}')
                
#             with self.lock:
#                 self.write_to_json(page_data, JSON_FILE)

#             with self.lock:
#                 self.counter += 1
#             # print(f'Crawled: {url}\tQueue Size: {self.q.qsize()}\tCounter: {self.counter}\n')
#             sleep(THREAD_DELAY)

#     def crawl(self, start_url=None, threadcount=THREADS, limit=LIMIT):
#         self.limit = limit
#         if start_url:
#             self.populate_queue(start_url)

#         for _ in range(threadcount):
#             self.thread_pool.append(CrawlerThread(target = self.thread_crawl))
        
#         for thread in self.thread_pool:
#             thread.start()

#         signal.signal(signal.SIGINT, self.stop_signal_handler)
                
#     def stop_threads(self):
#         for thread in self.thread_pool:
#             thread.stop_thread()

#     def join_threads(self):
#         for thread in self.thread_pool:
#             thread.join()

#     def save_queue(self):
#         queue_drain = []
#         while not self.q.empty():
#             queue_drain.append(self.q.get())

#         with open('queue.pkl', 'wb') as f:
#             pickle.dump(queue_drain, f)

#     def stop_signal_handler(self, sig, frame):
#         self.stop_threads()
#         self.join_threads()
#         print('\nTorcrawler QUITTING ...\n')
#         self.save_queue()


#     def __enter__(self):
#         return self

#     def __exit__(self, exc_type, exc_val, exc_tb):
#         pass

import requests
from stem import Signal
from stem.control import Controller
import threading
from queue import Queue
import random
from time import sleep
from requests.exceptions import HTTPError, ConnectTimeout
from urllib3.exceptions import MaxRetryError, ConnectTimeoutError
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse
import signal, pickle, os
import json
from config.CrawlerInstance import crawlerinstance
import asyncio
from pymongo import MongoClient

# db = MongoClient('mongodb://127.0.0.1:27017')
# kavach = db['kavach']
client = MongoClient('mongodb+srv://yash23malode:9dtb8MGh5aCZ5KHN@cluster.u0gqrzk.mongodb.net/')
db = client['prakat23']


PASSWORD = 'ligmaballs'
SOCKS_PORT = 9051
MAX_RETRIES = 3
RETRY_DELAY = 5
THREAD_DELAY = 1
# LIMIT = 1000
THREADS = 10
JSON_FILE = 'data.json'


class CrawlerThread(threading.Thread):
    
    def __init__(self, target=None, args=(), kwargs=None):
        super().__init__()
        self.target = target
        self.args = args
        self.kwargs = kwargs if kwargs is not None else {}
        self.stop_signal = False

    def run(self):
        if self.target:
            self.target(self, *self.args, **self.kwargs)
        print('Thread stopping')

    def stop_thread(self):
        self.stop_signal = True
        

class Crawler(): 

    def __init__(self, crawlerId = ''):
        self.counter = 0
        self.limit = 1000
        self.q = Queue()
        self.crawlerId = crawlerId
        self.user_q = Queue()
        self.crawled_links = []
        self.thread_pool = []
        self.lock = threading.Lock()

        self.controller = Controller.from_port(port=SOCKS_PORT)
        self.controller.authenticate(password=PASSWORD)

        print(f"Tor is running version {self.controller.get_version()}\n")

        self.proxies = {
            'http': 'socks5h://127.0.0.1:9050',
            'https': 'socks5h://127.0.0.1:9050'
        }

        with open('user-agents.txt', 'r') as f:
            uals = f.read().splitlines()
        
        self.user_agents = uals

        # if 'data.json' in os.listdir():
        #     self.update_crawled()

        # if 'queue.pkl' in os.listdir():
        #     with open('queue.pkl', 'rb') as f:
        #         queue_list = pickle.load(f)
        #     print('QUEUE PICKLE FOUND... POPULATING QUEUE FROM STORAGE...\n')
        #     for element in queue_list:
        #         self.q.put(element)

        

    def update_crawled(self):
        with open('data.json','r') as f:
            json_list = self.get_json_lines(f)
        for document_dict in json_list:
            if 'url' in document_dict.keys():
                self.crawled_links.append(document_dict['url'])


    def get_json_lines(self, file_obj):
        json_list = []
        for line in file_obj:
            json_line = json.loads(line.strip())
            json_list.append(json_line)
        return json_list

    def new_identity(self):
        self.controller.signal(Signal.NEWNYM)
        sleep(self.controller.get_newnym_wait())
        # print('NEW IDENTITY\n')

    def write_to_json(self, data, filename):
        if not(filename in os.listdir()):
            with open(filename, 'w') as f:
                f.write(json.dumps(data)+'\n')
        else: 
            with open(filename, 'a') as f:
                f.write(json.dumps(data)+'\n')


    def request_onion(self, url, retries=MAX_RETRIES, delay=RETRY_DELAY):
        newid_flag = 0
        for _ in range(retries):    
            ua = random.choice(self.user_agents)
            headers = {'User-Agent':ua} 
            try:
                response = requests.get(url,proxies=self.proxies, headers=headers)
                response.cookies.clear()
                print(f'REQUESTED {url} : {response.status_code}')
                if response.ok:
                    return response
                else:
                    response.raise_for_status()
            except (HTTPError, MaxRetryError, ConnectTimeoutError, ConnectTimeout) as e:
                # print(f'ERROR with {url} : {e}\nERROR Type: {type(e)}\n\n')
                if newid_flag:
                    break
                self.new_identity()
                newid_flag = 1
            except Exception as e:
                # print(f'ERROR with {url} : {type(e)}\n')
                break
            sleep(delay)

    def process_link(self, link, base_netloc, base_scheme):
        linkparse = urlparse(link)
        scheme = linkparse.scheme
        netloc = linkparse.netloc
        path = linkparse.path
        params = linkparse.params
        query = linkparse.query
        fragment = ''

        if not bool(path):
            path = '/'
        elif path == '/':
            pass
        elif path.endswith('/'):
            path = path[:-1]

        if not bool(scheme):
            scheme = base_scheme

        if not bool(netloc):
            netloc = base_netloc

        if not netloc.endswith('.onion'):
            scheme = 'https'
            
        return urlunparse((scheme, netloc, path, params, query, fragment))
   
    def extract_links(self, onion_resp, url):
        parse_dict = urlparse(url)
        base_netloc = parse_dict.netloc
        base_scheme = parse_dict.scheme
        if onion_resp is None:
            return
        soup = BeautifulSoup(onion_resp.text, 'lxml')
        
        anchor_list = soup.find_all("a")
        link_list = []
        for anchor in anchor_list:
            try:
                processed_link = self.process_link(anchor["href"], base_netloc, base_scheme)
                linkparse = urlparse(processed_link)
                if not linkparse.netloc.endswith('.onion'):
                    continue
                if processed_link != '':
                    link_list.append(processed_link)
            except:
                continue
        return link_list
    
    def get_data(self, onion_resp, url):
        onion_data = {}
        soup = BeautifulSoup(onion_resp.text, 'lxml')
        onion_data['url'] = url
        if soup.title:
            onion_data['title'] = soup.title.text
        else: 
            onion_data['title'] = ''
        onion_data['body'] = soup.body.get_text()
        onion_data['response-headers'] = dict(onion_resp.headers)
        return onion_data
    
    def populate_queue(self, url):
        try:
            response = self.request_onion(url)
        except Exception as e:
            # print(f'Connection with {url} FAILED...\n')
            print(f'ERROR: {e}\t{type(e)}')
        if (response is None) or (response.text is None):
            print(f'Inside populate_queue ERROR: LOCATION RETURNED NONE\n')
            db['crawlerinstance'].update_one({ "_id": self.crawlerId }, { "$set": { "status": 4 } })
        
        self.q.put(url)
        self.crawled_links.append(url)
        links = self.extract_links(response, url)
        if links is None:
            return
        for link in links:
            if not (link in self.crawled_links):
                self.q.put(link)
        print(f'QUEUE [size = {self.q.qsize()}] POPULATED WITH LINKS FROM {url}\n')

    def populate_user_queue(self, url):
        try:
            response = self.request_onion(url)
        except Exception as e:
            # print(f'Connection with {url} FAILED...\n')
            print(f'ERROR: {e}\t{type(e)}')
        if (response is None) or (response.text is None):
            print(f'ERROR: LOCATION RETURNED NONE\n')
        
        self.user_q.put(url)
        self.crawled_links.append(url)
        links = self.extract_links(response, url)
        for link in links:
            if not (link in self.crawled_links):
                self.user_q.put(link)
        # print(f'USER QUEUE POPULATED WITH LINKS FROM {url}\n')
         
    def thread_crawl(self, thread):
        while ((self.q.empty() == False) and (self.counter < self.limit)) or ((self.user_q.empty() == False) and (self.counter < self.limit)):

            if thread.stop_signal:
                break
            if (self.user_q.empty() == False):
                url = self.user_q.get()
            else:
                url = self.q.get()
            with self.lock:                   
                if url in self.crawled_links:
                    continue
                self.crawled_links.append(url)
            try:
                response = self.request_onion(url)
            except Exception as e:
                print(f'Connection with {url} FAILED...\n')
                # print(f'ERROR: {e}\t{type(e)}')
                continue
            if response is None:
                print(f'ERROR: LOCATION RETURNED NONE\n')
                continue

            links = self.extract_links(response, url)
            
            with self.lock:
                for link in links:
                    if not (link in self.crawled_links):
                        self.q.put(link)
            
            # EXTRACTING INFORMATION FROM URL RESPONSE  
            try:          
                page_data = self.get_data(response, url)
            except Exception as e:
                print(f'ERROR extracting data: {e}')
                
            with self.lock:
                self.write_to_json(page_data, JSON_FILE)

            with self.lock:
                self.counter += 1
            # print(f'Crawled: {url}\tQueue Size: {self.q.qsize()}\tCounter: {self.counter}\n')
            sleep(THREAD_DELAY)

        if self.counter >= self.limit:
            print("count: ", self.counter, " is greater than limit: ", self.limit)
            # doc = kavach['crawlerinstance'].find_one({ "_id": self.crawlerId })
            db['crawlerinstance'].update_one({ "_id": self.crawlerId }, { "$set": { "status": 3 } })
            print('updated status')

    def crawl(self, start_url=None, threadcount=THREADS, limit=1000):
        print('started crawling url ', start_url)
        db['crawlerinstance'].update_one({ "_id": self.crawlerId }, { "$set": { "status": 1 } })
        self.limit = limit
        if start_url:
            self.populate_queue(start_url)

        for _ in range(threadcount):
            self.thread_pool.append(CrawlerThread(target = self.thread_crawl))
        
        for thread in self.thread_pool:
            thread.start()

        # signal.signal(signal.SIGINT, self.stop_signal_handler)
                
    def stop_threads(self):
        for thread in self.thread_pool:
            thread.stop_thread()

    def join_threads(self):
        for thread in self.thread_pool:
            thread.join()

    def save_queue(self):
        queue_drain = []
        while not self.q.empty():
            queue_drain.append(self.q.get())

        with open('queue.pkl', 'wb') as f:
            pickle.dump(queue_drain, f)

    def stop_signal_handler(self, sig, frame):
        self.stop_threads()
        self.join_threads()
        print('\nTorcrawler QUITTING ...\n')
        self.save_queue()


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

