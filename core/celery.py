from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from kombu import Exchange, Queue

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'csv_importer.settings')

app = Celery('csv_importer')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related config keys should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Queue configuration
default_exchange = Exchange('default', type='direct')

app.conf.task_queues = (
    Queue('default', default_exchange, routing_key='default'),
)

app.conf.update(
    # Task execution settings
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=10,  # Increased from 1 to prevent frequent worker recycling
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_concurrency=3,
    
    # Broker settings
    broker_connection_timeout=30,
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=None,
    
    # Result backend settings
    result_backend='redis://localhost:6379/0',
    result_expires=3600,  # Results expire after 1 hour
    
    # Task routing
    task_default_queue='default',
    task_default_exchange='default',
    task_default_routing_key='default',
    
    # Error handling
    task_soft_time_limit=1800,  # 30 minutes
    task_time_limit=2100,      # 35 minutes
    
    # Performance optimizations
    worker_lost_wait=30,
    worker_disable_rate_limits=False,
)

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

# @app.task(bind=True)
# def debug_task(self):
#     print('Request: {0!r}'.format(self.request))
