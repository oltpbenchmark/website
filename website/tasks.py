import json
from django.http import HttpResponse
from celery import task
from gp import gp_tf
import numpy as np
from settings import * 
from gp import gp_tf 
import sklearn.preprocessing


@task(rate_limit='50/m')
def run_ml(X,Y,new_knobs):  #run machine learning code
    # do something
    logger = run_ml.get_logger(logfile='log/celery.log')
    logger.info('new knobs: {}'.format(new_knobs))
    model_X = sklearn.preprocessing.StandardScaler(X)
    model_y = sklearn.preprocessing.StandardScaler(Y)

    X_ = model_X.fit_transform(X)
    y_ = model_y.fit_transform(Y)


    X = np.array(X)
    y = np.array(Y)        
    #print X
    #print y
    sample_size = X.shape[0]
    #print X.shape
    #print y.shape 

    max_X = np.max(X,0)
    min_X = np.min(X,0)
   
    Xs = [] 
    #print min_X
    #print max_X
   
    for k in range(30):  #Xs_row number
        Xs_row = [] 
        for i in range(len(max_X)):
            Xs_row.append(np.random.randint(min_X[i],max_X[i]+1)) 
        Xs.append(Xs_row)
 
    Xs_ = model_X.fit_transform(Xs)
    
    # print Xs
    Xs = np.array(Xs_)
    ridges = np.random.uniform(0,1,[sample_size]) 



    yhats,sigmas, minL, confs = gp_tf(X_,y_,Xs_,ridges)

       

    index = np.argmin(minL)
    new_conf = confs[index] 
    logger.info("NEW CONF: {}".format(new_conf))
    new_conf = model_X.inverse_transform(new_conf)   

 
    res = {}
#     res['variable_names'] = str(new_knobs)
#     res['variable_values'] = str(new_conf).replace('\n',"")
    for i in range(len(new_knobs)):
        res[str(new_knobs[i])] = '10' #str(new_conf[i])
    res = json.dumps(res) 
  
 
    print res

    print ("Finished  !")
    return res




