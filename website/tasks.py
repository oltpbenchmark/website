from django.http import HttpResponse
from celery import task
import time
import urllib
import urllib2
import httplib
from poster.encode import multipart_encode
from poster.streaminghttp import register_openers
register_openers()

@task()
def run_ml(file):  #run machine learning code
    time.sleep(60)

    # do something 

    
    # get new configuration
    new_conf = "Here are the results"
#    x = 1/0
    return new_conf


    print ("Finished  !")


