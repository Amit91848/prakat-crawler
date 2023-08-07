from celery import Celery

celery_app: Celery = Celery(
    'torcrawler',
    broker='pyamqp://guest@localhost//',
)
