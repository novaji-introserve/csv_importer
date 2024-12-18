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
4. Start Celery worker: `celery -A core.celery worker -l info`
5. Run Django server: `python manage.py runserver`

For the django app itself
        sudo systemctl start csv_importer

        sudo systemctl stop csv_importer

        sudo systemctl restart csv_importer

        sudo systemctl status csv_importer


For the celery worker

        sudo systemctl start csv_importer_celery

        sudo systemctl stop csv_importer_celery

        sudo systemctl restart csv_importer_celery

        sudo systemctl status csv_importer_celery



For running the celery using the manage.sh script

        # Start Celery
        sudo celery-manage.sh start

        # View logs in real-time
        sudo celery-manage.sh logs

        # Check active tasks
        sudo celery-manage.sh inspect-active

        # Check active tasks
        sudo celery-manage.sh inspect-scheduled

        # Check active tasks
        sudo celery-manage.sh purge -f

        # Check active tasks
        sudo celery-manage.sh inspect-reserved


Redis is used as a broker for the celery 

Stop csv_importer_celery service before using the purge command

    sudo systemctl stop csv_importer_celery
    sudo celery-manage.sh purge
    sudo systemctl start csv_importer_celery



celery -A core.celery inspect active_queues
celery -A core.celery purge
celery -A core.celery worker --loglevel=debug
celery -A core.celery inspect active
celery -A core.celery inspect scheduled
celery -A core.celery status
celery -A core.celery worker --loglevel=info

TO collect static files
        python manage.py collectstatic



sudo -i -u postgres
TRUNCATE TABLE core_importlog RESTART IDENTITY;
SELECT * FROM core_importlog;
SELECT * FROM civil_servant_category;
SELECT * FROM civil_servant;
SELECT * FROM loan_details;
SELECT * FROM repayment;

UPDATE civil_servant
SET gender = 
    CASE
        WHEN gender ILIKE 'Male' THEN 'male'
        WHEN gender ILIKE 'Female' THEN 'female'
        ELSE gender
    END;


PREVIOUS SERVICE CONFIG

(.venv) [opc@lcc-431371 csv_importer]$ sudo cat /etc/systemd/system/csv_importer_celery.service
[Unit]
Description=Celery Worker Service for CSV Importer
After=network.target

[Service]
Type=forking
User=opc
Group=apache
WorkingDirectory=/var/www/html/csv_importer
Environment="PATH=/var/www/html/csv_importer/.venv/bin:/usr/bin"
EnvironmentFile=-/etc/sysconfig/celery

Environment="DJANGO_SETTINGS_MODULE=csv_importer.settings"

ExecStart=/var/www/html/csv_importer/.venv/bin/celery multi start worker \
    -A core.celery \
    --pidfile=/var/www/html/csv_importer/celery_%n.pid \
    --logfile=/var/www/html/csv_importer/logs/celery_%n%I.log \
    --loglevel=DEBUG \
    -Q default,celery \
    --max-tasks-per-child=1000 \
    ${CELERY_OPTS}

ExecStop=/var/www/html/csv_importer/.venv/bin/celery multi stopwait worker \
    --pidfile=/var/www/html/csv_importer/celery_%n.pid

ExecReload=/bin/kill -HUP $MAINPID

# Create directories
RuntimeDirectory=celery
RuntimeDirectoryMode=0755

Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target

