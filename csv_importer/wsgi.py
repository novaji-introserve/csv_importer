"""
WSGI config for csv_importer project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/5.1/howto/deployment/wsgi/
"""

import os
import sys
from pathlib import Path

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# Add the project directory to the Python path
sys.path.insert(0, str(BASE_DIR))

# Add the virtualenv site-packages to the Python path
VENV_PATH = BASE_DIR / '.venv' / 'lib' / 'python3.10' / 'site-packages'
sys.path.insert(1, str(VENV_PATH))

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'csv_importer.settings')

from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()
