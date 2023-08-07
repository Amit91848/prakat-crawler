import pika
import aio_pika

COMMAND_CREATE_CRAWLER = 'create_crawler'
COMMAND_STOP_CRAWLER = 'stop_crawler'
COMMAND_STATUS_CRAWLER = 'status_crawler'
COMMAND_TERMINATE_CRAWLER = 'terminate_crawler'

# class RabbitMQ:
#     def __init__(self):
#         self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
#         self.channel = self.connection.channel()
#         self.crawl_queue = self.channel.queue_declare(queue='custom_crawl_queue')
#         print(' [*] Waiting for messages. To exit press CTRL+C')

#     def start_basic_consume(self, queue, callback, auto_ack):
#         self.channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=auto_ack)

#     def start_consuming(self):
#         self.channel.start_consuming()

# class RabbitMQ:
#     def __init__(self):
#         self.connection = await aio_pika.connection.connect(host='localhost')

async def connect_rabbitmq():
    return await aio_pika.connect("amqp://guest:guest@127.0.0.1/")