import time
import urllib
import urllib2
import httplib
from poster.encode import multipart_encode
from poster.streaminghttp import register_openers
register_openers()


def run_ml(file):  #run machine learning code
    time.sleep(2)
    # finish and output

    f = open('/Users/zbh/Desktop/git/website/new_conf','w')
    for chunk in file['sample_data'].chunks():
        f.write(chunk)
    f.close()

    return "zbh"


    #f.write(file)
    #f.close()
    print ("Finished  !")
