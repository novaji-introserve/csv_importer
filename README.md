# Large CSV Import System

## Features

- Secure CSV file uploads
- Background processing with Celery
- Rate limiting
- Detailed import logging
- PostgreSQL bulk import optimizations

## Setup

1. Install dependencies: `pip install -r requirements.txt`
2. Configure database settings
3. Run migrations: `python manage.py migrate`
4. Start Celery worker: `celery -A csv_importer worker -l info`
5. Run Django server: `python manage.py runserver`
