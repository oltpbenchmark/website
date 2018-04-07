"""
WSGI config for OtterTune
"""

import os
import sys

import django.conf

from django.core.wsgi import get_wsgi_application


# Change the env variable where django looks for the settings module
# http://stackoverflow.com/a/11817088
django.conf.ENVIRONMENT_VARIABLE = "DJANGO_OLTPBENCH_SETTINGS_MODULE"
os.environ.setdefault("DJANGO_OLTPBENCH_SETTINGS_MODULE", "website.settings")
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

application = get_wsgi_application()
