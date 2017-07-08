import string
import re
import time
import json
import math
from random import choice
import numpy as np
from pytz import timezone, os
from rexec import FileWrapper

import logging
from django.core.exceptions import ObjectDoesNotExist
log = logging.getLogger(__name__)

import xml.dom.minidom
from django.template.context_processors import csrf
# from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
# from django.views.decorators.cache import cache_page
#from django.core.context_processors import csrf
from django.core.cache import cache
from django.shortcuts import redirect, render, get_object_or_404
from django.contrib.auth import login, logout
from django.contrib.auth.forms import AuthenticationForm, UserCreationForm
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, QueryDict
from django.template.defaultfilters import register
from django.utils.datetime_safe import datetime
from django.views.decorators.csrf import csrf_exempt
from django.utils.timezone import now

from models import * 
from website.settings import UPLOAD_DIR

from tasks import run_ml
from gp_workload import gp_workload

# For the html template to access dict object
@register.filter
def get_item(dictionary, key):
    return dictionary.get(key)

def ajax_new(request):
    new_id = request.GET['new_id']
    ts = Statistics.objects.filter(result=new_id)
    data = {}
    for metric in PLOTTABLE_FIELDS:   
        if len(ts) > 0:
            offset = ts[0].time
            if len(ts) > 1:
                offset -= ts[1].time - ts[0].time
            data[metric] = []
            for t in ts:
                data[metric].append([t.time - offset, getattr(t, metric) * METRIC_META[metric]['scale']])
    return HttpResponse(json.dumps(data), content_type = 'application/json')

def signup_view(request):
    if request.user.is_authenticated():
        return redirect('/')
    if request.method == 'POST':
        post = request.POST
        form = UserCreationForm(post)
        if form.is_valid():
            form.save()
            new_post = QueryDict(mutable=True)
            new_post.update(post)
            new_post['password'] = post['password1']
            request.POST = new_post
            return login_view(request)
        else:
            log.info("Invalid request: {}".format(', '.join(form.error_messages)))
        
    else:
        form = UserCreationForm()
    token = {}
    token.update(csrf(request))
    token['form'] = form
    
    return render(request, 'signup.html', token)

def login_view(request):
    if request.user.is_authenticated():
        return redirect('/')
    if request.method == 'POST':
        post = request.POST
        form = AuthenticationForm(None, post)
        if form.is_valid():
            login(request, form.get_user())
            return redirect('/')
        else:
            log.info("Invalid request: {}".format(', '.join(form.error_messages)))
    else:
        form = AuthenticationForm()
    token = {}
    token.update(csrf(request))
    token['form'] = form

    return render(request, 'login.html', token)

@login_required(login_url='/login/')
def logout_view(request):
    logout(request)
    return redirect("/login/")

def upload_code_generator(size=6, chars=string.ascii_uppercase + string.digits):
    # We must make sure this code does not already exist in the database
    # although duplicates should be extremely rare.
    new_upload_code = ''.join(choice(chars) for _ in range(size))
    num_dup_codes = Project.objects.filter(upload_code = new_upload_code).count()
    while (num_dup_codes > 0):
        new_upload_code = ''.join(choice(chars) for _ in range(size))
        num_dup_codes = Project.objects.filter(upload_code = new_upload_code).count()
    return new_upload_code


@login_required(login_url='/login/')
def home(request):
    context = {"projects": Project.objects.filter(user=request.user)}
    context.update(csrf(request))
    return render(request, 'home.html', context)


@login_required(login_url='/login/')
def ml_info(request):
#     id = request.GET['id'] 
    res = Result.objects.get(pk=id)
    task = Task.objects.get(pk=id)
    if task.running_time != None:
        time = task.running_time
    else:
        time = now() - res.creation_time
        time = time.seconds
    
    #limit = Website_Conf.objects.get(name = "Time_Limit")
    context = {"id":id,
               "result":res,
               "time":time,
               "task":task,
               "limit":"300"} #limit.value}

    return render(request,"ml_info.html",context) 

@login_required(login_url='/login/')
def project(request):
    id = request.GET['id']
    applications = Application.objects.filter(project = id)  
    project = Project.objects.get(pk=id)
    context = {"applications" : applications,
               "project" : project,
               "proj_id":id}
    context.update(csrf(request))
    return render(request, 'home_application.html', context)    

@login_required(login_url='/login/')
def project_info(request):
    id = request.GET['id']
    project = Project.objects.get(pk=id)
    context = {}
    context['project'] = project
    return render(request, 'project_info.html', context)

@login_required(login_url='/login/')
def application(request):
    id = request.GET['id']
    p = Application.objects.get(pk=id)
    if p.user != request.user:
        return render(request, '404.html')

    project = p.project

    data = request.GET

    application = Application.objects.get(pk=data['id'])
    print "data['id'] = {}".format(data['id'])
    print "APPLICATION: {}".format(application)
    results = Result.objects.filter(application=application)
#     results = Result.objects.select_related("db_conf__db_type","benchmark_conf__benchmark_type").filter(application=application)
#     results = Result.objects.prefetch_related("db_conf__db_type","benchmark_conf__benchmark_type").filter(application=application)
    print "RESULTS: {}".format(len(results))
#     db_with_data = {}
    benchmark_with_data = {}
    dbs = []

    for res in results:
        #db_with_data[res.db_conf.db_type] = True
        dbs.append(res.db_conf.dbms.name)
        benchmark_with_data[res.benchmark_conf.benchmark_type] = True
    benchmark_confs = set([res.benchmark_conf for res in results])
    #dbs = [db for db in DBConf.DB_TYPES if db in db_with_data]
    benchmark_types = benchmark_with_data.keys()
    benchmarks = {}
    for benchmark in benchmark_types:
        specific_benchmark = [b for b in benchmark_confs if b.benchmark_type == benchmark]
        benchmarks[benchmark] = specific_benchmark

    lastrevisions = [10, 50, 200, 1000]

    filters = []
#     for field in ExperimentConf.FILTER_FIELDS:
#         value_dict = {}
#         for res in results:
#             value_dict[getattr(res.benchmark_conf, field['field'])] = True
#         f = {'values': [key for key in value_dict.iterkeys()], 'print': field['print'], 'field': field['field']}
#         filters.append(f)

    context = {'project': project,
               'db_types': dbs,
               'benchmarks': benchmarks,
               'lastrevisions': lastrevisions,
               'defaultlast': 10,
               'defaultequid': False,
               'defaultbenchmark': 'grid',
               'metrics': PLOTTABLE_FIELDS,
               'metric_meta': METRIC_META,
               'defaultmetrics': ['throughput', 'p99_latency'],
               'filters': filters,
               'application':application, 
               'results': results} #Result.objects.filter(application=application)}

    context.update(csrf(request))
    return render(request, 'application.html', context)

@login_required(login_url='/login/')
def edit_project(request):
    context = {}
    try:
        if request.GET['id'] != '':
            project = Project.objects.get(pk=request.GET['id'])
            if project.user != request.user:
                return render(request, '404.html')
            context['project'] = project
    except Project.DoesNotExist:
        pass
    return render(request, 'edit_project.html', context)


@login_required(login_url='/login/')
def edit_application(request):
    context = {}
    try:
        if request.GET['id'] != '':
            application = Application.objects.get(pk=request.GET['id'])
            if application.user != request.user:
                return render(request, '404.html')
            context['application'] = application
    except Application.DoesNotExist:
        pass
    try:
        if request.GET['pid'] != '':
            project = Project.objects.get(pk=request.GET['pid'])
            context['project'] = project
    except Project.DoesNotExist:
        pass
    return render(request, 'edit_application.html', context)



@login_required(login_url='/login/')
def delete_project(request):
    for pk in request.POST.getlist('projects', []):
        project = Project.objects.get(pk=pk)
        if project.user == request.user:
            project.delete()
    return redirect('/')

@login_required(login_url='/login/')
def delete_application(request):
    for pk in request.POST.getlist('applications', []):
        application = Application.objects.get(pk=pk)
        if application.user == request.user:
            application.delete()
    id = request.POST['id'] 
    return redirect('/project/?id=' + id)


@login_required(login_url='/login/')
def update_project(request):
    gen_upload_code = False
    if 'id_new_code' in request.POST:
        proj_id = request.POST['id_new_code']
        gen_upload_code = True
    else:
        proj_id = request.POST['id']

    if proj_id == '':
        p = Project()
        p.creation_time = now()
        p.user = request.user
        gen_upload_code = True
        #p.upload_code = upload_code_generator(size=20)
    else:
        p = Project.objects.get(pk=proj_id)
        if p.user != request.user:
            return render(request, '404.html')

#     if 'id_new_code' in request.POST:
#         p.upload_code = upload_code_generator(size=20)
    if gen_upload_code:
        p.upload_code = upload_code_generator(size=20)

    p.name = request.POST['name']
    p.description = request.POST['description']
    p.last_update = now()
    p.save()
    applications = Application.objects.filter(project=p)
    
    context = {'project':p,
               'proj_id':p.pk,
               'applications':applications}
    return render(request, 'home_application.html', context)
   

def update_application(request):
    gen_upload_code = False
    if 'id_new_code' in request.POST:
        app_id = request.POST['id_new_code']
        gen_upload_code = True
    else:
        tmp = request.POST['id']
        tmp2 = tmp.split('&')
        app_id = tmp2[0]
        proj_id = tmp2[1]
    if app_id == '':
        p = Application()
        p.creation_time = now()
        p.user = request.user
        gen_upload_code = True
#         p.upload_code = upload_code_generator(size=20)
        p.project = Project.objects.get(pk=proj_id)
    else:
        p = Application.objects.get(pk=app_id)
        if p.user != request.user:
            return render(request, '404.html')

    if gen_upload_code:
        p.upload_code = upload_code_generator(size=20)
#     if 'id_new_code' in request.POST:
#         p.upload_code = upload_code_generator(size=20)

    p.name = request.POST['name']
    p.description = request.POST['description']
    p.last_update = now()
    p.save()
    return redirect('/application/?id=' + str(p.pk))

def write_file(contents, output_file, chunk_size = 512):
    des = open(output_file, 'w')
    for chunk in contents.chunks():
        des.write(chunk)
    des.close()

@csrf_exempt
def new_result(request):
    if request.method == 'POST':
        form = NewResultForm(request.POST, request.FILES)
        
        if not form.is_valid():
            log.warning("Form is not valid:\n"  + str(form))
            return HttpResponse("Form is not valid\n"  + str(form))
        try:   
            application = Application.objects.get(upload_code = form.cleaned_data['upload_code'])
        except Application.DoesNotExist:
            log.warning("Wrong upload code: " + form.cleaned_data['upload_code'])
            return HttpResponse("wrong upload_code!")
#         use = form.cleaned_data['upload_use']
#         hardware = form.cleaned_data['hardware']
#         cluster = form.cleaned_data['cluster']

        return handle_result_files(application, request.FILES, 'store')#,use,hardware,cluster)
    log.warning("Request type was not POST")
    return HttpResponse("POST please\n")

def get_result_data_dir(result_id):
    result_path = os.path.join(UPLOAD_DIR, str(result_id % 100))
    try:
        os.makedirs(result_path)
    except OSError as e:
        if e.errno == 17:
            pass
    return os.path.join(result_path, str(int(result_id) / 100l))


def process_config(cfg, knob_dict, summary):
    db_conf_lines = json.loads(cfg)
    db_globals = db_conf_lines['global'][0]
    db_cnf_names = db_globals['variable_names']
    db_cnf_values = db_globals['variable_values']
    row_x = []
    row_y = []
    for knob in knob_dict:
        # TODO (DVA): fixme
        if knob not in db_cnf_names:
            continue
        j = db_cnf_names.index(knob)
        value = str(db_cnf_values[j])
        value = value.lower().replace(".","")
        value = value.replace("-","")
        if value.isdigit() == False:
            s = knob_dict[knob]
            s = s.lower().split(',')
            s = sorted(s)
            #### NULL VALUE #####
            if value == "":
                value = -1
            else:
                value = s.index(value)
        row_x.append(float(value))

    summary_lines = json.loads(summary)
    sum_names = summary_lines['variable_names']
    sum_values = summary_lines['variable_values']
    row_y.append(float(sum_values[sum_names.index("99th_lat_ms")]))
    return row_x, row_y


def handle_result_files(app, files, use="", hardware="hardware",
                        cluster="cluster"):
    from .utils import parse_dbms_version_string
    
    # Load summary file
    summary = json.loads(''.join(files['summary_data'].chunks()))

    # Verify that the database/version is supported
    dbms_type = summary['DBMS Type'].upper()
    dbms_version = parse_dbms_version_string(dbms_type, summary['DBMS Version'])
    
    try:
        dbms_object = DBMSCatalog.objects.get(name=dbms_type, version=dbms_version)
    except ObjectDoesNotExist:
        return HttpResponse('{} v{} is not yet supported.'.format(dbms_type, dbms_version))
    
    # Load DB parameters file
    db_parameters = json.loads(''.join(files['db_parameters_data'].chunks()))
    
    # Load DB metrics file
    db_metrics = json.loads(''.join(files['db_metrics_data'].chunks()))
    
    # Load benchmark config file
    benchmark_config = ''.join(files['benchmark_conf_data'].chunks())
    
    # Load samples file
    samples = ''.join(files['sample_data'].chunks())

    # Load raw data file (not required)
    raw_data = ''
#     if 'raw_data' in files:
#         raw_data = ''.join( files['raw_data'].chunks())
#     else:
#         raw_data = ''


    # Create benchmark config model
    dom = xml.dom.minidom.parseString(benchmark_config)
    root = dom.documentElement
    benchmark_info = Workload_info()
    benchmark_info.isolation = (root.getElementsByTagName('isolation'))[0].firstChild.data
    benchmark_info.scalefactor = (root.getElementsByTagName('scalefactor'))[0].firstChild.data
    benchmark_info.terminals  = (root.getElementsByTagName('terminals'))[0].firstChild.data
    benchmark_info.time  = (root.getElementsByTagName('time'))[0].firstChild.data
    benchmark_info.rate  = (root.getElementsByTagName('rate'))[0].firstChild.data
    benchmark_info.skew  = (root.getElementsByTagName('skew'))
    if len(benchmark_info.skew) == 0:
        benchmark_info.skew = -1.0
    else:
        benchmark_info.skew = benchmark_info.skew[0].firstChild.data
    weights = root.getElementsByTagName('weights')
    trans = root.getElementsByTagName('name')
    trans_dict = {}
    for i in range(trans.length):
        trans_dict[trans[i].firstChild.data] = weights[i].firstChild.data
    trans_json = json.dumps(trans_dict)
    benchmark_info.trans_weights = trans_json
    benchmark_info.workload = benchmark_config
    benchmark_info.save()


    benchmark_type = summary['Benchmark Type'].upper()

    # Create new experiment model
    experiment_info = Oltpbench_info()
    experiment_info.summary = json.dumps(summary)
#     experiment_info.dbms_name = dbms_type
#     experiment_info.dbms_version = dbms_version
    experiment_info.dbms = dbms_object
    experiment_info.res = samples
    experiment_info.status = db_metrics
    experiment_info.raw = raw_data
    experiment_info.cfg = db_parameters
    experiment_info.wid = benchmark_info
    experiment_info.user =  app.user
    experiment_info.hardware = hardware 
    experiment_info.cluster = cluster

    experiment_info.save()

#     if use.lower() == 'store':
#         return HttpResponse( "Store Success !") 


#     knob_params = KNOB_PARAMS.objects.filter(db_type = dbms_type)
#     knob_dict = {}
#  
#     for x in knob_params:
#         name = x.params
#         tmp = Knob_catalog.objects.filter(name=name)
#         knob_dict[name] = tmp[0].valid_vals
#     
#     cfgs = Oltpbench_info.objects.filter(user=app.user)   
#   
#     target_Xs=[]
#     target_Ys=[]
#  
#     for x in cfgs:
#         target_x,target_y = process_config(x.cfg, knob_dict, x.summary)
#         target_Xs.append(target_x)
#         target_Ys.append(target_y)
#  
#     exps = Oltpbench_info.objects.filter(dbms_name=dbms_type,
#                                          dbms_version=dbms_version,
#                                          hardware = hardware)
# 
#     #print target_Xs
#     #print target_Ys
# 
# 
#     ### workload mapping 
# 
#     clusters_list= []
#     for x in exps: 
#         t = x.cluster
#         if t not in clusters_list and t != 'unknown':
#             clusters_list.append(t)
# 
# 
# 
#     
#     workload_min = []
#     X_min = []
#     Y_min = []
#     min_dist = 1000000
# 
#     for name in clusters_list:
#         exps_ = Oltpbench_info.objects.filter(dbms_name=dbms_type,dbms_version=dbms_version,hardware = hardware,cluster = name )       
#         
#         X=[]
#         Y=[]        
#         for x_ in exps_:
#             x,y = process_config(x_.cfg,knob_dict,x_.summary)
#             X.append(x)
#             Y.append(y)  
#    
#         sample_size = len(X)
#         ridges = np.random.uniform(0,1,[sample_size])
#         print "workload"
#         y_gp = gp_workload(X,Y,target_Xs,ridges) 
#         dist = np.sqrt(sum(pow(np.transpose(y_gp-target_Ys)[0],2)))
#         if min_dist > dist:
#             min_dist = dist
#             X_min = X
#             Y_min = Y
#             workload_min = name
# 
#     bench.cluster = workload_min 
#     bench.save()
# 
# 
# 
# 
#     globals = db_conf_lines['global']
#     globals = globals[0]
#  
#     db_cnf_names = globals['variable_names']
#     db_cnf_values = globals['variable_values']
#     db_cnf_info = {}
#     for i in range(len(db_cnf_names)):
#         db_cnf_info[db_cnf_names[i]] = db_cnf_values[i]
# 
#     if not dbms_type in DBConf.DB_TYPES:
#         return HttpResponse(dbms_type + "  db_type Wrong")

    featured_params = []
    for param in KnobCatalog.objects.filter(dbms=dbms_object, tunable=True):
        featured_params.append(re.compile(param.name, re.UNICODE | re.IGNORECASE))
 
#     features = LEARNING_PARAMS.objects.filter(db_type = dbms_type)
#     LEARNING_VARS = []
#     for f in features:
#         LEARNING_VARS.append( re.compile(f.params, re.UNICODE | re.IGNORECASE))
    db_conf_list = []
    similar_conf_list = []
#     for i in range(len(db_cnf_info)):
#         key = db_cnf_info.keys()[i]
#         value = db_cnf_info.values()[i]    
#         for v in LEARNING_VARS:
#             if v.match(key):
#                 similar_conf_list.append([key,value])
#         db_conf_list.append([key, value])
        
    for k,v in db_parameters.iteritems():
        for var in featured_params:
            if var.match(k):
                similar_conf_list.append([k, v])
        db_conf_list.append([k, v])
    #log.warn(db_conf_list)
    db_conf_str = json.dumps(db_conf_list)
    similar_conf_str = json.dumps(similar_conf_list)	
    

    try:
        db_confs = DBConf.objects.filter(configuration=db_conf_str, similar_conf=similar_conf_str)
        if len(db_confs) < 1:
            raise DBConf.DoesNotExist
        db_conf = db_confs[0]
    except DBConf.DoesNotExist:
        db_conf = DBConf()
        db_conf.creation_time = now()
        db_conf.name = ''
        db_conf.configuration = db_conf_str
        db_conf.application = app
        db_conf.dbms = dbms_object
        db_conf.similar_conf = similar_conf_str
        db_conf.save()
        db_conf.name = dbms_type + '@' + db_conf.creation_time.strftime("%Y-%m-%d,%H") + '#' + str(db_conf.pk)
        db_conf.save()
    bench_conf_str = "".join( files['benchmark_conf_data'].chunks())

    try:
        bench_confs = ExperimentConf.objects.filter(configuration=bench_conf_str)
        if len(bench_confs) < 1:
            raise ExperimentConf.DoesNotExist
        bench_conf = bench_confs[0]
    except ExperimentConf.DoesNotExist:
        bench_conf = ExperimentConf()
        bench_conf.name = ''
        bench_conf.application = app
        bench_conf.configuration = bench_conf_str
        bench_conf.benchmark_type = benchmark_type
        bench_conf.creation_time = now()
        bench_conf.isolation = summary['isolation'].upper()
        bench_conf.terminals = summary['terminals']
        bench_conf.scalefactor = summary['scalefactor']
        bench_conf.save()
        bench_conf.name = benchmark_type + '@' + bench_conf.creation_time.strftime("%Y-%m-%d,%H") + '#' + str(bench_conf.pk)
        bench_conf.save()

    result = Result()
    result.db_conf = db_conf
    result.benchmark_conf = bench_conf
    result.application = app
    result.timestamp = datetime.fromtimestamp(int(summary['Current Timestamp (milliseconds)']) / 1000, timezone("UTC"))

    latencies = summary['Latency Distribution']
    result.avg_latency = float(latencies['Average Latency (microseconds)'])
    result.min_latency = float(latencies['Minimum Latency (microseconds)'])
    result.p25_latency = float(latencies['25th Percentile Latency (microseconds)'])
    result.p50_latency = float(latencies['Median Latency (microseconds)'])
    result.p75_latency = float(latencies['75th Percentile Latency (microseconds)'])
    result.p90_latency = float(latencies['90th Percentile Latency (microseconds)'])
    result.p95_latency = float(latencies['95th Percentile Latency (microseconds)'])
    result.p99_latency = float(latencies['99th Percentile Latency (microseconds)'])
    result.max_latency = float(latencies['Maximum Latency (microseconds)'])
    result.throughput = float(summary['Throughput (requests/second)'])
    result.creation_time = now()
    result.save()

    path_prefix = get_result_data_dir(result.pk)
    with open(path_prefix + '_sample', 'wb') as dest:
        for chunk in files['sample_data'].chunks():
            dest.write(chunk)
        dest.close()
    
    # TODO (DVA): fixme
    with open(path_prefix + '_raw', 'wb') as dest:
        if 'raw_data' in files:
            for chunk in files['raw_data'].chunks():
                dest.write(chunk)
        else:
            dest.write('')
        dest.close()

    sample_lines = samples.split('\n')
    header = sample_lines[0].split(',')
    
    time_idx = header.index('Time (seconds)')
    tput_idx = header.index('Throughput (requests/second)')
    avg_idx = header.index('Average Latency (microseconds)')
    min_idx = header.index('Minimum Latency (microseconds)')
    p25_idx = header.index('25th Percentile Latency (microseconds)')
    p50_idx = header.index('Median Latency (microseconds)')
    p75_idx = header.index('75th Percentile Latency (microseconds)')
    p90_idx = header.index('90th Percentile Latency (microseconds)')
    p95_idx = header.index('95th Percentile Latency (microseconds)')
    p99_idx = header.index('99th Percentile Latency (microseconds)')
    max_idx = header.index('Maximum Latency (microseconds)')
    for line in sample_lines[1:]:
        if line == '':
            continue
        sta = Statistics()
        nums = line.strip().split(',')
        log.warn(nums)
        sta.result = result
        sta.time = int(nums[time_idx])
        sta.throughput = float(nums[tput_idx])
        sta.avg_latency = float(nums[avg_idx])
        sta.min_latency = float(nums[min_idx])
        sta.p25_latency = float(nums[p25_idx])
        sta.p50_latency = float(nums[p50_idx])
        sta.p75_latency = float(nums[p75_idx])
        sta.p90_latency = float(nums[p90_idx])
        sta.p95_latency = float(nums[p95_idx])
        sta.p99_latency = float(nums[p99_idx])
        sta.max_latency = float(nums[max_idx])
        sta.save()
 
    app.project.last_update = now()
    app.last_update = now()
    app.project.save()
    app.save()

    return HttpResponse( "Store Success !") 

#     id = result.pk
#     task = Task()
#     task.id = id
#     task.creation_time = now() 
#     print "run_ml"
#     response = run_ml.delay(X_min,Y_min,knob_dict.keys() )
#     task.status = response.status
#     task.save()
#     #time limits  default  300s 
#     time_limit =  Website_Conf.objects.get(name = 'Time_Limit')
#     time_limit = int(time_limit.value)
#     
#     for i in range(time_limit):
#         time.sleep(1)
#         if response.status != task.status:
#             task.status = response.status
#             task.save()
#         if response.ready():
#             task.finish_time = now()
#             break
#     
#     response_message = task.status
#     if task.status == "FAILURE":
#         task.traceback = response.traceback
#         task.running_time = (task.finish_time - task.creation_time).seconds
#         response_message += ": " + response.traceback
#     elif task.status == "SUCCESS":
#         res = response.result
#         with open(path_prefix + '_new_conf', 'wb') as dest:        
#             dest.write(res)
#             dest.close()
#  
#         task.running_time = (task.finish_time - task.creation_time).seconds
#         task.result = res
#         task.save()
#         return HttpResponse(res) 
#     else:
#         task.status = "TIME OUT"
#         task.traceback = response.traceback 
#         task.running_time = time_limit
#         response_message = "TIME OUT: " + response.traceback
#     task.save()
# #     return  HttpResponse(task.status)
#     return  HttpResponse(response_message)


def file_iterator(file_name, chunk_size=512):
    with open(file_name) as f:
        while True:
            c = f.read(chunk_size)
            if c:
                yield c
            else:
                break

def filter_db_var(kv_pair, key_filters):
    for f in key_filters:
        if f.match(kv_pair[0]):
            return True
    return False

@login_required(login_url='/login/')
def db_conf_view(request):
    db_conf = DBConf.objects.get(pk=request.GET['id'])
    if db_conf.application.user != request.user:
        return render(request, '404.html')
    dbms_config = json.loads(db_conf.configuration, encoding="UTF-8")

    tunable_param_names = []
    for param in KnobCatalog.objects.filter(dbms=DBConf.dbms, tunable=True):
        tunable_param_names.append(re.compile(param.name, re.UNICODE | re.IGNORECASE))
        
    tunable_params = filter(lambda x: filter_db_var(x, tunable_param_names), dbms_config)
#     tunable_params = []
#     for k,v in dbms_config.iteritems():
#         for param_name in tunable_param_names:
#             if param_name.match(k):
#                 tunable_params
#     features = FEATURED_PARAMS.objects.filter(db_type = db_conf.db_type)
#     FEATURED_VARS = []
#     for f in features:
#         tmp = re.compile(f.params, re.UNICODE | re.IGNORECASE)
#     	FEATURED_VARS.append(tmp)
 
#     featured = filter(lambda x: filter_db_var(x, FEATURED_VARS), conf)

    if 'compare' in request.GET and request.GET['compare'] != 'none':
        compare_conf = DBConf.objects.get(pk=request.GET['compare'])
        compare_conf_list = json.loads(compare_conf.configuration, encoding='UTF-8')
        for a, b in zip(dbms_config, compare_conf_list):
            a.extend(b[1:])
        for a, b in zip(tunable_params, filter(lambda x: filter_db_var(x, tunable_param_names),
                        json.loads(compare_conf.configuration, encoding='UTF-8'))):
            a.extend(b[1:])

    peer = DBConf.objects.filter(db_type=db_conf.db_type, application=db_conf.application)
    peer_db_conf = [[c.name, c.pk] for c in peer if c.pk != db_conf.pk]

    context = {'parameters': dbms_config,
               'featured_par': tunable_params,
               'db_conf': db_conf,
               'compare': request.GET.get('compare', 'none'),
               'peer_db_conf': peer_db_conf}
    return render(request, 'db_conf.html', context)


@login_required(login_url='/login/')
def benchmark_configuration(request):
    benchmark_conf = get_object_or_404(ExperimentConf, pk=request.GET['id'])

    if benchmark_conf.application.user != request.user:
        return render(request, '404.html')

    dbms_objects = DBMSCatalog.objects.all()
    all_db_confs = []
    dbs = {}
    for dbms_object in dbms_objects:
    #for db_type in DBConf.DB_TYPES:
        dbms_key = '{}_{}'.format(dbms_object.name, dbms_object.version)
        dbs[dbms_key] = {}

        db_confs = DBConf.objects.filter(application=benchmark_conf.application, db_type=dbms_object)
        for db_conf in db_confs:
            rs = Result.objects.filter(db_conf=db_conf, benchmark_conf=benchmark_conf)
            if len(rs) < 1:
                continue
            r = rs.latest('timestamp')
            all_db_confs.append(db_conf.pk)
            dbs[dbms_key][db_conf.name] = [db_conf, r]

        if len(dbs[dbms_key]) < 1:
            dbs.pop(dbms_key)

    context = {'benchmark': benchmark_conf,
               'dbs': dbs,
               'metrics': PLOTTABLE_FIELDS,
               'metric_meta': METRIC_META,
               'default_dbconf': all_db_confs,
               'default_metrics': ['throughput', 'p99_latency']}
    return render(request, 'benchmark_conf.html', context)

# Data Format
#    error
#    metrics as a list of selected metrics
#    results
#        data for each selected metric
#            meta data for the metric
#            Result list for the metric in a folded list
@login_required(login_url='/login/')
def get_benchmark_data(request):
    data = request.GET

    benchmark_conf = get_object_or_404(ExperimentConf, pk=data['id'])

    if benchmark_conf.application.user != request.user:
        return render(request, '404.html')

    results = Result.objects.filter(benchmark_conf=benchmark_conf)
    results = sorted(results, cmp=lambda x, y: int(y.throughput - x.throughput))

    data_package = {'results': [],
                    'error': 'None',
                    'metrics': data.get('met', 'throughput,p99_latency').split(',')}

    for met in data_package['metrics']:
        data_package['results']. \
            append({'data': [[]], 'tick': [],
                    'unit': METRIC_META[met]['unit'],
                    'lessisbetter': METRIC_META[met][
                                        'lessisbetter'] and '(less is better)' or '(more is better)',
                    'metric': METRIC_META[met]['print']})

        added = {}
        db_confs = data['db'].split(',')
        i = len(db_confs)
        for r in results:
            if r.db_conf.pk in added or str(r.db_conf.pk) not in db_confs:
                continue
            added[r.db_conf.pk] = True
            data_package['results'][-1]['data'][0].append(
                [i, getattr(r, met) * METRIC_META[met]['scale'], r.pk, getattr(r, met) * METRIC_META[met]['scale']])
            data_package['results'][-1]['tick'].append(r.db_conf.name)
            i -= 1
        data_package['results'][-1]['data'].reverse()
        data_package['results'][-1]['tick'].reverse()

    return HttpResponse(json.dumps(data_package), content_type='application/json')

@login_required(login_url='/login/')
def get_benchmark_conf_file(request):
    id = request.GET['id']
    benchmark_conf = get_object_or_404(ExperimentConf, pk=request.GET['id'])
    if benchmark_conf.application.user != request.user:
        return render(request, '404.html')

    response = HttpResponse(benchmark_conf.configuration, content_type='text/plain')
    response['Content-Disposition'] = 'attachment; filename=result_' + str(id) + '.ben.cnf'
    return response

@login_required(login_url='/login/')
def edit_benchmark_conf(request):
    context = {}
    if request.GET['id'] != '':
        ben_conf = get_object_or_404(ExperimentConf, pk=request.GET['id'])
        if ben_conf.application.user != request.user:
            return render(request, '404.html')
        context['benchmark'] = ben_conf
    return render(request, 'edit_benchmark.html', context)


@login_required(login_url='/login/')
def update_benchmark_conf(request):
    ben_conf = ExperimentConf.objects.get(pk=request.POST['id'])
    ben_conf.name = request.POST['name']
    ben_conf.description = request.POST['description']
    ben_conf.save()
    return redirect('/benchmark_conf/?id=' + str(ben_conf.pk))


def result_similar(a, b):
    db_conf_a = json.loads(a.db_conf.similar_conf)
    db_conf_b = json.loads(b.db_conf.similar_conf)
    for kv in db_conf_a:
        for bkv in db_conf_b:
            if bkv[0] == kv[0] and bkv[1] != kv[1]:
                return False
            else:
                break
    return True


def learn_model(results):
    features = []
    features2 = KnobCatalog.objects.filter(dbms=results[0].db_conf.dbms, tunable=True)
    LEARNING_VARS = []
    for f in features2:
        LEARNING_VARS.append( re.compile(f.params, re.UNICODE | re.IGNORECASE))

    for f in LEARNING_VARS:
        values = []
        for r in results:
            db_conf = json.loads(r.db_conf.configuration)
            for kv in db_conf:
                if f.match(kv[0]):
                    try:
                        values.append(math.log(int(kv[1])))
                        break
                    except Exception:
                        values.append(0.0)
                        break

        features.append(values)

    A = np.array(features)
    y = [r.throughput for r in results]
    w = np.linalg.lstsq(A.T, y)[0]

    return w


def apply_model(model, data, target):
    values = []
    db_conf = json.loads(data.db_conf.configuration)
    db_conf_t = json.loads(target.db_conf.configuration)
    features = KnobCatalog.objects.filter(dbms=data.db_conf.dbms, tunable=True)
    LEARNING_VARS = []
    for f in features:
        LEARNING_VARS.append( re.compile(f.params, re.UNICODE | re.IGNORECASE))
    
    for f in LEARNING_VARS:
        v1 = 0
        v2 = 0
        for kv in db_conf:
            if f.match(kv[0]):
                if kv[1] == '0':
                    kv[1] = '1'
                v1 = math.log(int(kv[1]))
        for kv in db_conf_t:
            if f.match(kv[0]):
                if kv[1] == '0':
                    kv[1] = '1'
                v2 = math.log(int(kv[1]))
        values.append(v1 - v2)

    score = 0
    for i in range(0, len(model)):
        score += abs(model[i] * float(values[i]))
    return score


@login_required(login_url='/login/')
def update_similar(request):
    target = get_object_or_404(Result, pk=request.GET['id'])
    results = Result.objects.filter(application=target.application, benchmark_conf=target.benchmark_conf)
    results = filter(lambda x: x.db_conf.dbms == target.db_conf.dbms, results)

    linear_model = learn_model(results)
    diff_results = filter(lambda x: x != target, results)
    diff_results = filter(lambda x: not result_similar(x, target), diff_results)
    scores = [apply_model(linear_model, x, target) for x in diff_results]
    similars = sorted(zip(diff_results, scores), cmp=lambda x, y: x[1] > y[1] and 1 or (x[1] < y[1] and -1 or 0))
    if len(similars) > 5:
        similars = similars[:5]

    target.most_similar = ','.join([str(r[0].pk) for r in similars])
    target.save()

    return redirect('/result/?id=' + str(request.GET['id']))



def result(request):
    target = get_object_or_404(Result, pk=request.GET['id'])
    #task = get_object_or_404(Task, id=request.GET['id'])
    data_package = {}
    sames = {}   
    similars = {}
  
    #results = Result.objects.select_related("db_conf__db_type","db_conf__configuration","db_conf__similar_conf").filter(application=target.application, benchmark_conf=target.benchmark_conf)
    results = Result.objects.select_related("db_conf__application").filter(application=target.application, benchmark_conf=target.benchmark_conf)

    results = filter(lambda x: x.db_conf.dbms == target.db_conf.dbms, results)
    #sames = []
    sames = filter(lambda x:  result_similar(x,target) and x != target , results)
  

    similars = [Result.objects.get(pk=rid) for rid in filter(lambda x: len(x) > 0, target.most_similar.split(','))]

    #results = []
    for metric in PLOTTABLE_FIELDS:
        data_package[metric] = {
            'data': {},
            'units': METRIC_META[metric]['unit'],
            'lessisbetter': METRIC_META[metric]['lessisbetter'] and '(less is better)' or '(more is better)',
            'metric': METRIC_META[metric]['print']
        }

        same_id = []
        same_id.append(str(target.pk))
        for x in same_id:   
            key = metric + ',data,' + x ;
            tmp = cache.get(key);
            if tmp != None:
                data_package[metric]['data'][int(x)] = []
                data_package[metric]['data'][int(x)].extend(tmp);
                continue;	

            ts = Statistics.objects.filter(result=x) 
            if len(ts) > 0:
                offset = ts[0].time
                if len(ts) > 1:
                    offset -= ts[1].time - ts[0].time
                data_package[metric]['data'][int(x)] = []
                for t in ts:
                    data_package[metric]['data'][int(x)].append(
                        [t.time - offset, getattr(t, metric) * METRIC_META[metric]['scale']])
                cache.set(key,data_package[metric]['data'][int(x)],60*5) 

    overall_throughput = '{0:0.2f}'.format(target.throughput * METRIC_META['throughput']['scale'])
    overall_p99_latency = '{0:0.2f}'.format(target.p99_latency * METRIC_META['p99_latency']['scale'])

    default_metrics = {}
    for met in ['throughput', 'p99_latency']:
        default_metrics[met] = '{0:0.2f}'.format(getattr(target, met) * METRIC_META[met]['scale'])
    
    context = {
        'result': target, 
        'metrics': PLOTTABLE_FIELDS,
        'metric_meta': METRIC_META,
        #'default_metrics': ['throughput', 'p99_latency'],
        'overall_throughput': overall_throughput,
        'overall_p99_latency': overall_p99_latency,
        'default_metrics': default_metrics,
        'data': json.dumps(data_package),
        'same_runs': sames,
        'task':'', #task,
        'similar_runs': similars
    }
    return render(request, 'result.html', context)


@login_required(login_url='/login/')
def get_result_data_file(request):
    target = get_object_or_404(Result, pk=request.GET['id'])
    #task =  get_object_or_404(Task, pk=request.GET['id'])
    if target.application.user != request.user:
        return render(request, '404.html')

    id = int(request.GET['id'])
    type = request.GET['type']

    prefix = get_result_data_dir(id)

    if type == 'sample':
        response = HttpResponse(FileWrapper(file(prefix + '_' + type)), content_type='text/plain')
        response.__setitem__('Content-Disposition', 'attachment; filename=result_' + str(id) + '.sample')
        return response
    elif type == 'raw':
        response = HttpResponse(FileWrapper(file(prefix + '_' + type)), content_type='text/plain')    
        response['Content-Disposition'] = 'attachment; filename=result_' + str(id) + '.raw'
        return response
    elif type == 'new_conf':
        response = HttpResponse(FileWrapper(file(prefix + '_' + type)), content_type='text/plain')
        response['Content-Disposition'] = 'attachment; filename=result_' + str(id) + '_new_conf'
        return response


#Data Format:
#    error
#    results
#        all result data after the filters for the table
#    timelines
#        data for each benchmark & metric pair
#            meta data for the pair
#            data as a map<DBMS name, result list>
@login_required(login_url='/login/')
def get_timeline_data(request):
    data_package = {'error': 'None', 'timelines': []}

    application = get_object_or_404(Application, pk=request.GET['proj'])
    if application.user != request.user:
        return HttpResponse(json.dumps(data_package), content_type='application/json')

    revs = int(request.GET['revs'])
    
    log.warn(request.GET['db'].split(','))

    # Get all results related to the selected DBMS, sort by time
    results = Result.objects.filter(application=request.GET['proj'])
    results = filter(lambda x: x.db_conf.dbms.name in request.GET['db'].split(','), results)
    results = sorted(results, cmp=lambda x, y: int((x.timestamp - y.timestamp).total_seconds()))

    # Determine which benchmark is selected
    benchmarks = []
    if request.GET['ben'] == 'grid':
        revs = 10
        table_results = []
    elif request.GET['ben'] == 'show_none':
        benchmarks = []
        table_results = []
    else:
        benchmarks = [request.GET['ben']]
        benchmark_confs = filter(lambda x: x != '', request.GET['spe'].strip().split(','))
        results = filter(lambda x: str(x.benchmark_conf.pk) in benchmark_confs, results)

        for f in filter(lambda x: x != '', request.GET.getlist('add[]', [])):
            key, value = f.split(':')
            if value == 'select_all':
                continue
            results = filter(lambda x: getattr(x.benchmark_conf, key) == value, results)

        table_results = results

    if len(benchmarks) == 1:
        metrics = request.GET.get('met', 'throughput,p99_latency').split(',')
    else:
        metrics = ['throughput']

    # For the data table
    data_package['results'] = [
        [x.pk,
         x.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
         x.db_conf.name,
         x.benchmark_conf.name,
         x.throughput * METRIC_META['throughput']['scale'],
         x.p99_latency * METRIC_META['p99_latency']['scale'],
         x.db_conf.pk,
         x.benchmark_conf.pk
        ]
        for x in table_results]

    # For plotting charts
    for metric in metrics:
        for bench in benchmarks:
            b_r = filter(lambda x: x.benchmark_conf.benchmark_type == bench, results)
            if len(b_r) == 0:
                continue

            data = {
                'benchmark': bench,
                'units': METRIC_META[metric]['unit'],
                'lessisbetter': METRIC_META[metric]['lessisbetter'] and '(less is better)' or '(more is better)',
                'data': {},
                'baseline': "None",
                'metric': metric
            }

            for db in request.GET['db'].split(','):
                d_r = filter(lambda x: x.db_conf.dbms.name == db, b_r)
                d_r = d_r[-revs:]
                out = [
                    [
                        res.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                        getattr(res, metric) * METRIC_META[metric]['scale'],
                        "",
                        str(res.pk)
                    ]
                    for res in d_r]

                if len(out) > 0:
                    data['data'][db] = out

            data_package['timelines'].append(data)

    return HttpResponse(json.dumps(data_package), content_type='application/json')
