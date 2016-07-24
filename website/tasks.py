from django.http import HttpResponse
from celery import task
import time
import urllib
import urllib2
import httplib
from poster.encode import multipart_encode
from poster.streaminghttp import register_openers
register_openers()

@task(rate_limit='10/m')
def run_ml(file):  #run machine learning code
    time.sleep(60)

    # do something 

    
    # get new configuration
    new_conf = "Here is the result ..... \n \n "
#    x = 1/0
    return new_conf


    print ("Finished  !")


