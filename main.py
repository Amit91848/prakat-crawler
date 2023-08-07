from message_queue.rabbitmq import COMMAND_STOP_CRAWLER, COMMAND_CREATE_CRAWLER, COMMAND_STATUS_CRAWLER, COMMAND_TERMINATE_CRAWLER
import json
from manager.crawler_manager import CrawlerManager
from config.CrawlerInstance import crawlerinstance
import asyncio
import aio_pika
from bson import ObjectId

# crawler_manager = CrawlerManager()

# # http://catalogpwwlccc5nyp3m3xng6pdx3rdcknul57x6raxwf4enpw3nymqd.onion/buy/1696
# # http://qy6gxhomey5wgxojueukonptxrltv5ppc37qv3vlgjkkbv2yiflggnid.onion/index.html
# # url = input("Enter the start url for the crawler -->")
# crawler = Crawler()
# def callback(ch, method, properties, body):
#     global crawler
#     # data = json.loads(body)
#     url = body.decode('utf-8')


#     # command = data['command']
#     # body = data['body']

#     # url = body['url']
#     # limit = body['limit']
#     # userid = body['userid']
    
#     # if command == COMMAND_START_CRAWLER:
#     #     crawler_manager.start_crawler(url=url, userid=userid ,limit=limit)
#     # else:
#     #     print(command + " this command does not exist")
        
#     print(f" [x] Received {url}")
#     crawler.new_identity()
#     # threading.Thread(target=crawler.crawl, args=body)
#     # crawler.crawl(url)
#     crawler.populate_user_queue(url)
#     # crawler.new_identity()
    
#     # crawler.crawl(body)
    

# # crawler = Crawler()
# # crawler.new_identity()
# # if url == '':
# #     crawler.crawl()
# # else:
# # crawler.crawl("https://www.nytimesn7cgmftshazwhfgzm37qxb44r64ytbb2dj3x62d2lljsciiyd.onion/")

# # @celery_app.task
# # def crawl(url: str):
# #     return "will start crawling: " + url

# # def start_rabbit():
# #     rabbitmq = RabbitMQ()


# #     rabbitmq.start_basic_consume('custom_crawl_queue', callback, True)
# #     rabbitmq.start_consuming()

# def main():
#     # rabbit_thread = threading.Thread(target=start_rabbit, daemon=True)
#     # rabbit_thread.start()
#     # start_rabbit()

#     # rabbit_process = Process(target=start_rabbit)
    
#     # crawler.crawl("https://www.nytimesn7cgmftshazwhfgzm37qxb44r64ytbb2dj3x62d2lljsciiyd.onion/")
#     # crawler_thread = threading.Thread(target=crawler.crawl, args=("https://www.nytimesn7cgmftshazwhfgzm37qxb44r64ytbb2dj3x62d2lljsciiyd.onion/",))
#     # crawler_thread.start()
#     crawler.crawl("http://catalogpwwlccc5nyp3m3xng6pdx3rdcknul57x6raxwf4enpw3nymqd.onion/buy/1696")


#     # crawler_manager = CrawlerManager()

#     # Start a new crawler for a given URL

#     # with Crawler() as crawler:
#     #     crawler.new_identity()
        
#     #     if not url:
#     #         crawler.crawl()
#     #     else:
#     #         crawler.crawl(url)
            
# if __name__ == "__main__":
#     main()


from multiprocessing import Process, Queue
import pika
from torcrawler import Crawler
from config.config import initiate_database
from pymongo import MongoClient


# def rabbitmq_consumer(queue, crawler_manager: CrawlerManager):
#     connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
#     channel = connection.channel()

#     channel.queue_declare(queue='custom_crawl_queue')

#     def callback(ch, method, properties, body):
#         data = json.loads(body)
#         print(data)

#         command = data['command']
#         crawlerId = data['crawlerId']

#         crawler_doc = CrawlerInstance.get(crawlerId)

#         if command == COMMAND_CREATE_CRAWLER:
#             crawler_manager.create_crawler(crawler_doc=crawler_doc)
#         elif command == COMMAND_STOP_CRAWLER:
#             crawler_manager.stop_crawler(crawler_doc=crawler_doc)


#     print(' [*] Waiting for messages. To exit press CTRL+C')
#     channel.basic_consume(queue='custom_crawl_queue', on_message_callback=callback, auto_ack=True)
#     channel.start_consuming()
# async def process_message(body):
#     crawler_manager = CrawlerManager()
#     data = json.loads(body)
#     print(data)

#     command = data['command']
#     crawlerId = data['crawlerId']

#     # Use CrawlerInstance.objects instead of CrawlerInstance.get to get the document
#     crawler_doc = await crawlerinstance.get(crawlerId)
#     print(crawler_doc)


    # if command == COMMAND_CREATE_CRAWLER:
    #     await crawler_manager.create_crawler(crawler_doc=crawler_doc)
    # elif command == COMMAND_STOP_CRAWLER:
    #     await crawler_manager.stop_crawler(crawler_doc=crawler_doc)

# def rabbitmq_consumer(queue, crawler_manager: CrawlerManager):
#     connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
#     channel = connection.channel()

#     channel.queue_declare(queue='custom_crawl_queue')


#     def callback(ch, method, properties, body):
#         asyncio.run(process_message(body))

#     print(' [*] Waiting for messages. To exit press CTRL+C')
#     channel.basic_consume(queue='custom_crawl_queue', on_message_callback=callback, auto_ack=True)
#     channel.start_consuming()

async def process_message(message: aio_pika.abc.AbstractIncomingMessage):
    print(' [*] Waiting for messages. To exit press CTRL+C')
    async with message.process():
        print(message.body)
        crawler_manager = CrawlerManager()
        data = json.loads(message.body)
        print(data)

        command = data['command']
        crawlerId = data['crawlerId']

        # Use CrawlerInstance.objects instead of CrawlerInstance.get to get the document
        crawler_doc = await crawlerinstance.get(ObjectId(crawlerId))

        if command == COMMAND_CREATE_CRAWLER:
            crawler_manager.create_crawler(crawler_doc=crawler_doc)
        elif command == COMMAND_STOP_CRAWLER:
            crawler_manager.stop_crawler(crawler_doc=crawler_doc)
    
async def connect_rabbitmq() -> None:
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@127.0.0.1/",
    )

    channel = await connection.channel()

    # # Maximum message count which will be processing at the same time.
    # await channel.set_qos(prefetch_count=100)

    # Declaring queue
    # queue = await channel.declare_queue(queue_name)

    # await queue.consume(process_message)
    queue = await channel.declare_queue('custom_crawl_queue')
    print('declare queue')

    await queue.consume(process_message)
    

    try:
        # Wait until terminate
        await asyncio.Future()
    finally:
        await connection.close()



if __name__ == "__main__":
    # asyncio.run(initiate_database())
    # asyncio.run(connect_rabbitmq())

    async def main():
        await asyncio.gather(initiate_database(), connect_rabbitmq())

    asyncio.run(main())

    # queue = Queue()
    # crawler_manager = CrawlerManager()
    # p1 = Process(target=rabbitmq_consumer, args=(queue, crawler_manager), name='Rabbitmq Process')
    # # p2 = Process(target=crawler_process, args=(queue,), name='Crawler Process')
    p2 = Process(target=CrawlerManager, name="Crawler Manager Process")
    # p1.start()
    p2.start()
    # p1.join()

    # p2.join()


# def crawler_process(queue):
#     my_crawler = Crawler()  # Assuming Crawler is a class in crawler module
#     my_crawler.crawl('https://www.nytimesn7cgmftshazwhfgzm37qxb44r64ytbb2dj3x62d2lljsciiyd.onion/')
#     while True:
#         task = queue.get()
#         if task is None:
#             break
#         my_crawler.populate_user_queue(task)  # Assuming crawl is a method that takes a task and crawls

# command = data.get('command')
        # url = data.get('url')
        # limit = data.get('limit')
        # same_domain = data.get('same_domain')
        # crawlerId = data.get('crawlerId')

        # if command == COMMAND_CREATE_CRAWLER:
        #     crawler_manager.create_crawler(url=url, crawlerId=crawlerId, same_domain=same_domain, limit=limit)
        # elif command == COMMAND_STOP_CRAWLER:
        #     crawler_manager.stop_crawler(crawlerId)
        # else:
        #     print('Wrong option')