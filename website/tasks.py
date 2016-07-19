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
    time.sleep(20)

    # do something 
    f = open('/Users/zbh/Desktop/git/website/new_conf','w')
    for chunk in file['sample_data'].chunks():
        f.write(chunk)
    f.close()

    
    # get new configuration
    new_conf = "zbh"
#    x = 1/0
    return new_conf


    print ("Finished  !")


