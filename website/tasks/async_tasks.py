import time

from celery.task import task, Task
from django.utils.timezone import now

from .models import Task as TaskModel


class UpdateTask(Task):

    def __call__(self, *args, **kwargs):
        self.rate_limit = '50/m'
        self.max_retries = 3
        self.default_retry_delay = 60
        
        # Update start time for this task
        task = TaskModel.objects.get(taskmeta_id=self.request.id)
        task.start_time = now()
        task.save()
        return super(UpdateTask, self).__call__(*args, **kwargs)
    
#     def after_return(self, status, retval, task_id, args, kwargs, einfo):
#         super(UpdateTask, self).after_return(status, retval, task_id, args, kwargs, einfo)
#         print "RETURNED!! (task_id={}, rl={}, mr={}, drt={})".format(task_id, self.rate_limit, self.max_retries, self.default_retry_delay)
#     
#     def on_failure(self, exc, task_id, args, kwargs, einfo):
#         super(UpdateTask, self).on_failure(exc, task_id, args, kwargs, einfo)
#         print "FAILURE!! {} (task_id={})".format(exc, task_id)
#     
#     def on_success(self, retval, task_id, args, kwargs):
#         super(UpdateTask, self).on_success(retval, task_id, args, kwargs)
#         print "SUCCESS!! result={} (task_id={})".format(retval, task_id)
#     
#     def on_retry(self, exc, task_id, args, kwargs, einfo):
#         super(UpdateTask, self).on_retry(exc, task_id, args, kwargs, einfo)
#         print "RETRY!! {} (task_id={})".format(exc, task_id)

@task(base=UpdateTask, name='preprocess')
def preprocess(a, b):
    print "PREPROCESSING ({}, {})".format(a, b)
    time.sleep(2)
    return a + b

@task(base=UpdateTask, name='run_wm')
def run_wm(q, r):
    print "RUNNING WM: ({}, {})".format(q, r)
    time.sleep(3)
    return q + r

@task(base=UpdateTask, name='run_gpr')
def run_gpr(x, y):
    print "RUNNING GP ({}, {})".format(x, y)
    time.sleep(4)
    return x + y


    
#     # do something
#     logger = run_ml.get_logger(logfile='log/celery.log')
#     logger.info('new knobs: {}'.format(new_knobs))
#     model_X = sklearn.preprocessing.StandardScaler(X)
#     model_y = sklearn.preprocessing.StandardScaler(Y)
# 
#     X_ = model_X.fit_transform(X)
#     y_ = model_y.fit_transform(Y)
# 
# 
#     X = np.array(X)
#     y = np.array(Y)        
#     #print X
#     #print y
#     sample_size = X.shape[0]
#     #print X.shape
#     #print y.shape 
# 
#     max_X = np.max(X,0)
#     min_X = np.min(X,0)
#    
#     Xs = [] 
#     #print min_X
#     #print max_X
#    
#     for k in range(30):  #Xs_row number
#         Xs_row = [] 
#         for i in range(len(max_X)):
#             Xs_row.append(np.random.randint(min_X[i],max_X[i]+1)) 
#         Xs.append(Xs_row)
#  
#     Xs_ = model_X.fit_transform(Xs)
#     
#     # print Xs
#     Xs = np.array(Xs_)
#     ridges = np.random.uniform(0,1,[sample_size]) 
# 
# 
# 
#     yhats,sigmas, minL, confs = gp_tf(X_,y_,Xs_,ridges)
# 
#        
# 
#     index = np.argmin(minL)
#     new_conf = confs[index] 
#     logger.info("NEW CONF: {}".format(new_conf))
#     new_conf = model_X.inverse_transform(new_conf)   
# 
#  
#     res = {}
# #     res['variable_names'] = str(new_knobs)
# #     res['variable_values'] = str(new_conf).replace('\n',"")
#     for i in range(len(new_knobs)):
#         res[str(new_knobs[i])] = '10' #str(new_conf[i])
#     res = json.dumps(res) 
#   
#  
#     print res
# 
#     print ("Finished  !")
#     return res




