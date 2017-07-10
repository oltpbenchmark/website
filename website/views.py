import string
import re
import time
#import json
import math
from random import choice
import numpy as np
from pytz import timezone, os
from rexec import FileWrapper

import logging
from django.core.exceptions import ObjectDoesNotExist
from collections import OrderedDict
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
from django.http import HttpResponse, QueryDict, Http404
from django.template.defaultfilters import register
from django.utils.datetime_safe import datetime
from django.views.decorators.csrf import csrf_exempt
from django.utils.timezone import now

from .utils import JSONUtil
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
#     return HttpResponse(json.dumps(data), content_type = 'application/json')
    return HttpResponse(JSONUtil.dumps(data), content_type = 'application/json')

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
    project_id = request.GET['id']
    applications = Application.objects.filter(project = project_id)  
    project = Project.objects.get(pk=project_id)
    context = {"applications" : applications,
               "project" : project,
               "proj_id":project_id}
    context.update(csrf(request))
    return render(request, 'home_application.html', context)    

@login_required(login_url='/login/')
def project_info(request):
    project_id = request.GET['id']
    project = Project.objects.get(pk=project_id)
    context = {}
    context['project'] = project
    return render(request, 'project_info.html', context)

@login_required(login_url='/login/')
def application(request):
    p = Application.objects.get(pk=request.GET['id'])
    if p.user != request.user:
        return render(request, '404.html')

    project = p.project

    data = request.GET

    application = Application.objects.get(pk=data['id'])
    results = Result.objects.filter(application=application)
#     results = Result.objects.select_related("db_conf__db_type","benchmark_conf__benchmark_type").filter(application=application)
#     results = Result.objects.prefetch_related("db_conf__db_type","benchmark_conf__benchmark_type").filter(application=application)
#     db_with_data = {}
#     benchmark_with_data = {}
    dbs = {}
    benchmarks = {}

    for res in results:
        dbs[res.dbms.key] = res.dbms
        bench_type = res.benchmark_config.benchmark_type
        if bench_type not in benchmarks:
            benchmarks[bench_type] = set()
        benchmarks[bench_type].add(res.benchmark_config)
    
    benchmarks = {k: sorted(list(v)) for k,v in benchmarks.iteritems()}
    benchmarks = OrderedDict(sorted(benchmarks.iteritems()))
#         benchmark_with_data[res.benchmark_config.benchmark_type] = True
#     benchmark_confs = set([res.benchmark_config for res in results])
#     benchmark_types = benchmark_with_data.keys()
#     benchmarks = {}
#     for benchmark in benchmark_types:
#         specific_benchmark = [b for b in benchmark_confs if b.benchmark_type == benchmark]
#         benchmarks[benchmark] = specific_benchmark

    lastrevisions = [10, 50, 100]
    dbs = OrderedDict(sorted(dbs.items()))
    filters = []
#     for field in BenchmarkConfig.FILTER_FIELDS:
#         value_dict = {}
#         for res in results:
#             value_dict[getattr(res.benchmark_conf, field['field'])] = True
#         f = {'values': [key for key in value_dict.iterkeys()], 'print': field['print'], 'field': field['field']}
#         filters.append(f)
    context = {'project': project,
               'dbmss': dbs,
               'benchmarks': benchmarks,
               'lastrevisions': lastrevisions,
               'defaultdbms': "none" if len(dbs) == 0 else dbs.keys()[0], 
               'defaultlast': 10,
               'defaultequid': False,
               'defaultbenchmark': 'grid',
               'defaultspe': "none" if len(benchmarks) == 0 else list(benchmarks.iteritems())[0][0], 
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
        # TODO (dva): FIXME
        hardware_type = HardwareType.EC2_M3XLARGE
        if hardware_type == HardwareType.GENERIC:
            raise NotImplementedError('Implement me!')
        else:
            p.hardware = Hardware.objects.get(type=hardware_type)
        gen_upload_code = True
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
        upload_code = form.cleaned_data['upload_code']
        try:   
            application = Application.objects.get(upload_code=upload_code)
        except Application.DoesNotExist:
            log.warning("Wrong upload code: " + upload_code)
            return HttpResponse("wrong upload_code!")

        return handle_result_files(application, request.FILES, 'store')
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
    db_conf_lines = JSONUtil.loads(cfg)
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

    summary_lines = JSONUtil.loads(summary)
    sum_names = summary_lines['variable_names']
    sum_values = summary_lines['variable_values']
    row_y.append(float(sum_values[sum_names.index("99th_lat_ms")]))
    return row_x, row_y


def handle_result_files(app, files, use="", hardware="hardware",
                        cluster="cluster"):
#     from .types import DBMSType
    from .utils import DBMSUtil
    
    # Load summary file
#     summary = json.loads(''.join(files['summary_data'].chunks()))
    summary = JSONUtil.loads(''.join(files['summary_data'].chunks()))

    # Verify that the database/version is supported
    dbms_type = DBMSType.type(summary['DBMS Type'])
    dbms_version = DBMSUtil.parse_version_string(dbms_type, summary['DBMS Version'])
    
    try:
        dbms_object = DBMSCatalog.objects.get(type=dbms_type, version=dbms_version)
    except ObjectDoesNotExist:
        return HttpResponse('{} v{} is not yet supported.'.format(summary['DBMS Type'], dbms_version))
    
    # Load DB parameters file
#     db_parameters = json.loads(''.join(files['db_parameters_data'].chunks()))
    db_parameters = JSONUtil.loads(''.join(files['db_parameters_data'].chunks()))
    
    # Load DB metrics file
#     db_metrics = json.loads(''.join(files['db_metrics_data'].chunks()))
    db_metrics = JSONUtil.loads(''.join(files['db_metrics_data'].chunks()))

    
    # Load benchmark config file
    benchmark_config_str = ''.join(files['benchmark_conf_data'].chunks())
    
    # Load samples file
    samples = ''.join(files['sample_data'].chunks())

    benchmark_configs = BenchmarkConfig.objects.filter(configuration=benchmark_config_str)
    if len(benchmark_configs) >= 1:
        benchmark_config = benchmark_configs[0]
    else:
        benchmark_config = BenchmarkConfig()
        benchmark_config.name = ''
        benchmark_config.application = app
        benchmark_config.configuration = benchmark_config_str
        benchmark_config.benchmark_type = summary['Benchmark Type'].upper()
        benchmark_config.creation_time = now()

        dom = xml.dom.minidom.parseString(benchmark_config_str)
        root = dom.documentElement
        benchmark_config.isolation = (root.getElementsByTagName('isolation'))[0].firstChild.data
        benchmark_config.scalefactor = (root.getElementsByTagName('scalefactor'))[0].firstChild.data
        benchmark_config.terminals  = (root.getElementsByTagName('terminals'))[0].firstChild.data
        benchmark_config.time  = (root.getElementsByTagName('time'))[0].firstChild.data
        benchmark_config.rate  = (root.getElementsByTagName('rate'))[0].firstChild.data
        benchmark_config.skew  = (root.getElementsByTagName('skew'))
        benchmark_config.skew = -1 if len(benchmark_config.skew) == 0 else benchmark_config.skew[0].firstChild.data
        benchmark_config.transaction_types = [t.firstChild.data for t in root.getElementsByTagName('name')]
        benchmark_config.transaction_weights = [w.firstChild.data for w in root.getElementsByTagName('weights')]
        benchmark_config.save()
        benchmark_config.name = benchmark_config.benchmark_type + '@' + \
                benchmark_config.creation_time.strftime("%Y-%m-%d,%H") + \
                '#' + str(benchmark_config.pk)
        benchmark_config.save()

    db_conf_dict = DBMSUtil.parse_dbms_config(dbms_object.type,
                                              db_parameters,
                                              KnobCatalog.objects.filter(dbms=dbms_object))
    db_conf_str = JSONUtil.dumps(db_conf_dict, pprint=True)
    tunable_param_names = [re.compile(p.name, re.UNICODE | re.IGNORECASE) \
                           for p in KnobCatalog.objects.filter(dbms=dbms_object, tunable=True)]
    tunable_params = filter(lambda x: filter_db_var(x, tunable_param_names), list(db_conf_dict.iteritems()))
    tunable_params = {k:v for k,v in tunable_params}

    creation_time = now()
    db_confs = DBConf.objects.filter(configuration=db_conf_str, application=app)
    if len(db_confs) >= 1:
        db_conf = db_confs[0]
    else:
        db_conf = DBConf()
        db_conf.creation_time = creation_time
        db_conf.name = ''
        db_conf.configuration = db_conf_str
        db_conf.tuning_configuration = JSONUtil.dumps(tunable_params, pprint=True)
        db_conf.raw_configuration = JSONUtil.dumps(db_parameters)
        db_conf.application = app
        db_conf.dbms = dbms_object
        db_conf.description = ''
        db_conf.save()
        db_conf.name = dbms_object.key + '@' + creation_time.strftime("%Y-%m-%d,%H") + '#' + str(db_conf.pk)
        db_conf.save()
    
    
    db_metrics_str = JSONUtil.dumps(DBMSUtil.parse_dbms_metrics(dbms_object.type,
                                                                db_metrics,
                                                                MetricCatalog.objects.filter(dbms=dbms_object)),
                                    pprint=True)
    dbms_metrics = DBMSMetrics()
    dbms_metrics.creation_time = creation_time
    dbms_metrics.name = ''
    dbms_metrics.configuration = db_metrics_str
    dbms_metrics.raw_configuration = JSONUtil.dumps(db_metrics)
    dbms_metrics.execution_time = benchmark_config.time
    dbms_metrics.application = app
    dbms_metrics.dbms = dbms_object
    dbms_metrics.save()
    dbms_metrics.name = dbms_object.key + '@' + creation_time.strftime("%Y-%m-%d,%H") + '#' + str(dbms_metrics.pk)
    dbms_metrics.save()

    result = Result()
    result.application = app
    result.dbms = dbms_object
    result.dbms_config = db_conf
    result.dbms_metrics = dbms_metrics
    result.benchmark_config = benchmark_config
    
    result.summary = JSONUtil.dumps(summary, pprint=True)
    result.samples = samples

    result.timestamp = datetime.fromtimestamp(int(summary['Current Timestamp (milliseconds)']) / 1000, timezone("UTC"))
    result.hardware = hardware
    result.cluster = cluster

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

    path_prefix = get_result_data_dir(result.pk)
    with open('{}.samples'.format(path_prefix), 'w') as f:
        for chunk in files['sample_data'].chunks():
            f.write(chunk)
    
    with open('{}.summary'.format(path_prefix), 'w') as f:
        for chunk in files['summary_data'].chunks():
            f.write(chunk)

    with open('{}.params'.format(path_prefix), 'w') as f:
        for chunk in files['db_parameters_data'].chunks():
            f.write(chunk)

    with open('{}.metrics'.format(path_prefix), 'w') as f:
        for chunk in files['db_metrics_data'].chunks():
            f.write(chunk)

    with open('{}.expconfig'.format(path_prefix), 'w') as f:
        for chunk in files['benchmark_conf_data'].chunks():
            f.write(chunk)
    
    if 'raw_data' in files:
        with open('{}.csv.tgz'.format(path_prefix), 'w') as f:
            for chunk in files['raw_data'].chunks():
                f.write(chunk)

    return HttpResponse( "Store Success !") 
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
def dbms_metrics_view(request):
    dbms_metrics = get_object_or_404(DBMSMetrics, pk=request.GET['id'])
    if dbms_metrics.application.user != request.user:
        raise Http404()
    metrics = JSONUtil.loads(dbms_metrics.configuration)

    normalized_metrics = []
    for metric_info in MetricCatalog.objects.filter(dbms=dbms_metrics.dbms):
        mname = metric_info.name
        
        if metric_info.metric_type == MetricType.COUNTER:
            mvalue = float(metrics[mname]) / dbms_metrics.execution_time
        else:
            mvalue = '-'
        normalized_metrics.append((mname, mvalue))

    context = {'metrics': list(metrics.iteritems()),
               'normalized_metrics': normalized_metrics,
               'dbms_metrics': dbms_metrics,
               'compare': request.GET.get('compare', 'none'),
               'peer_dbms_metrics': []}
    return render(request, 'dbms_metrics.html', context)

@login_required(login_url='/login/')
def db_conf_view(request):
    db_conf = get_object_or_404(DBConf, pk=request.GET['id'])
    if db_conf.application.user != request.user:
        raise Http404()
    dbms_config = list(JSONUtil.loads(db_conf.configuration).iteritems())
    tuning_config = list(JSONUtil.loads(db_conf.tuning_configuration).iteritems())

    if 'compare' in request.GET and request.GET['compare'] != 'none':
        compare_conf = DBConf.objects.get(pk=request.GET['compare'])
        compare_conf_list = JSONUtil.loads(compare_conf.configuration, encoding='UTF-8')
        for a, b in zip(dbms_config, compare_conf_list):
            a.extend(b[1:])
        for a, b in zip(tuning_config, filter(lambda x: filter_db_var(x, [t[0] for t in tuning_config]),
                        JSONUtil.loads(compare_conf.configuration, encoding='UTF-8'))):
            a.extend(b[1:])

#     peer = DBConf.objects.filter(dbms=db_conf.dbms, application=db_conf.application)
#     peer_db_conf = [[c.name, c.pk] for c in peer if c.pk != db_conf.pk]
    peer_db_conf = []

    context = {'parameters': dbms_config,
               'featured_par': tuning_config,
               'db_conf': db_conf,
               'compare': request.GET.get('compare', 'none'),
               'peer_db_conf': peer_db_conf}
    return render(request, 'db_conf.html', context)


@login_required(login_url='/login/')
def benchmark_configuration(request):
    benchmark_conf = get_object_or_404(BenchmarkConfig, pk=request.GET['id'])

    if benchmark_conf.application.user != request.user:
        return render(request, '404.html')

    dbms_objects = DBMSCatalog.objects.all()
    all_db_confs = []
    dbs = {}
    for dbms_object in dbms_objects:
        dbms_name = dbms_object.full_name
        dbs[dbms_name] = {}

#         db_confs = DBConf.objects.filter(application=benchmark_conf.application, dbms=dbms_object)
        db_confs = DBConf.objects.filter(dbms=dbms_object)
        for db_conf in db_confs:
            rs = Result.objects.filter(dbms_config=db_conf, benchmark_config=benchmark_conf)
            if len(rs) < 1:
                continue
            r = rs.latest('timestamp')
            all_db_confs.append(db_conf.pk)
            dbs[dbms_name][db_conf.name] = [db_conf, r]

        if len(dbs[dbms_name]) < 1:
            dbs.pop(dbms_name)

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

    benchmark_conf = get_object_or_404(BenchmarkConfig, pk=data['id'])

    if benchmark_conf.application.user != request.user:
        return render(request, '404.html')

    results = Result.objects.filter(benchmark_config=benchmark_conf)
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
        log.warn('DB CONFS!!! {}'.format(db_confs))
        i = len(db_confs)
        for r in results:
            if r.dbms_config.pk in added or str(r.dbms_config.pk) not in db_confs:
                continue
            added[r.dbms_config.pk] = True
            data_package['results'][-1]['data'][0].append(
                [i, getattr(r, met) * METRIC_META[met]['scale'], r.pk, getattr(r, met) * METRIC_META[met]['scale']])
            data_package['results'][-1]['tick'].append(r.dbms_config.name)
            i -= 1
        data_package['results'][-1]['data'].reverse()
        data_package['results'][-1]['tick'].reverse()

#     return HttpResponse(json.dumps(data_package), content_type='application/json')
    return HttpResponse(JSONUtil.dumps(data_package), content_type='application/json')

@login_required(login_url='/login/')
def get_benchmark_conf_file(request):
    id = request.GET['id']
    benchmark_conf = get_object_or_404(BenchmarkConfig, pk=request.GET['id'])
    if benchmark_conf.application.user != request.user:
        return render(request, '404.html')

    response = HttpResponse(benchmark_conf.configuration, content_type='text/plain')
    response['Content-Disposition'] = 'attachment; filename=result_' + str(id) + '.ben.cnf'
    return response

@login_required(login_url='/login/')
def edit_benchmark_conf(request):
    context = {}
    if request.GET['id'] != '':
        ben_conf = get_object_or_404(BenchmarkConfig, pk=request.GET['id'])
        if ben_conf.application.user != request.user:
            return render(request, '404.html')
        context['benchmark'] = ben_conf
    return render(request, 'edit_benchmark.html', context)


@login_required(login_url='/login/')
def update_benchmark_conf(request):
    ben_conf = BenchmarkConfig.objects.get(pk=request.POST['id'])
    ben_conf.name = request.POST['name']
    ben_conf.description = request.POST['description']
    ben_conf.save()
    return redirect('/benchmark_conf/?id=' + str(ben_conf.pk))


def result_similar(a, b):
    db_conf_a = JSONUtil.loads(a.dbms_config.tuning_configuration)
    db_conf_b = JSONUtil.loads(b.dbms_config.tuning_configuration)
    for k,v in db_conf_a.iteritems():
        if k not in db_conf_b or v != db_conf_b[k]:
            return False
    return True
#         for bkv in db_conf_b:
#             if bkv[0] == kv[0] and bkv[1] != kv[1]:
#                 return False
#             else:
#                 break
#     return True

def result_same(a, b):
    db_conf_a = JSONUtil.loads(a.dbms_config.configuration)
    db_conf_b = JSONUtil.loads(b.dbms_config.configuration)
    for k,v in db_conf_a.iteritems():
        if k not in db_conf_b or v != db_conf_b[k]:
            return False
    return True


def learn_model(results):
    return 0
#     features = []
#     features2 = KnobCatalog.objects.filter(dbms=results[0].dbms, tunable=True)
#     LEARNING_VARS = []
#     for f in features2:
#         LEARNING_VARS.append( re.compile(f.name, re.UNICODE | re.IGNORECASE))
# 
#     for f in LEARNING_VARS:
#         values = []
#         for r in results:
#             db_conf = JSONUtil.loads(r.dbms_config.tuning_configuration)
#             for kv in db_conf:
#                 if f.match(kv[0]):
#                     try:
#                         values.append(math.log(int(kv[1])))
#                         break
#                     except Exception:
#                         values.append(0.0)
#                         break
# 
#         features.append(values)
# 
#     A = np.array(features)
#     y = [r.throughput for r in results]
#     return np.linalg.lstsq(A.T, y)[0]


def apply_model(model, data, target):
    return 0
#     values = []
#     db_conf = JSONUtil.loads(data.dbms_config.tunable_configuration)
#     db_conf_t = JSONUtil.loads(target.dbms_config.tunable_configuration)
#     features = KnobCatalog.objects.filter(dbms=data.dbms, tunable=True)
#     LEARNING_VARS = []
#     for f in features:
#         LEARNING_VARS.append( re.compile(f.name, re.UNICODE | re.IGNORECASE))
#     
#     for f in LEARNING_VARS:
#         v1 = 0
#         v2 = 0
#         for kv in db_conf:
#             if f.match(kv[0]):
#                 if kv[1] == '0':
#                     kv[1] = '1'
#                 v1 = math.log(int(kv[1]))
#         for kv in db_conf_t:
#             if f.match(kv[0]):
#                 if kv[1] == '0':
#                     kv[1] = '1'
#                 v2 = math.log(int(kv[1]))
#         values.append(v1 - v2)
# 
#     score = 0
#     for i in range(0, len(model)):
#         score += abs(model[i] * float(values[i]))
#     return score



@login_required(login_url='/login/')
def update_similar(request):
    target = get_object_or_404(Result, pk=request.GET['id'])
    results = Result.objects.filter(application=target.application, benchmark_config=target.benchmark_config)
    results = filter(lambda x: x.dbms == target.dbms, results)

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
#     sames = {}   
#     similars = {}
  
    #results = Result.objects.select_related("db_conf__db_type","db_conf__configuration","db_conf__similar_conf").filter(application=target.application, benchmark_conf=target.benchmark_conf)
#     results = Result.objects.select_related("dbms_config__dbms").filter(application=target.application, benchmark_config=target.benchmark_config)
    results = Result.objects.filter(application=target.application, dbms=target.dbms, benchmark_config=target.benchmark_config)

#     results = filter(lambda x: x.dbms == target.dbms, results)
    #sames = []
#     sames = filter(lambda x: result_similar(x, target) and x != target , results)
    same_dbconf_results = filter(lambda x: result_same(x, target) and x.pk != target.pk, results)
    similar_dbconf_results = filter(lambda x: result_similar(x, target) and \
                                    x.pk not in ([target.pk] + [r.pk for r in same_dbconf_results]), results)

#     similars = [Result.objects.get(pk=rid) for rid in filter(lambda x: len(x) > 0, target.most_similar.split(','))]

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

    default_metrics = {}
    for met in ['throughput', 'p99_latency']:
        default_metrics[met] = '{0:0.2f}'.format(getattr(target, met) * METRIC_META[met]['scale'])
    
    context = {
        'result': target, 
        'metrics': PLOTTABLE_FIELDS,
        'metric_meta': METRIC_META,
        'default_metrics': default_metrics,
        'data': JSONUtil.dumps(data_package),
        'same_runs': same_dbconf_results,
        'task':'', #task,
        'similar_runs': similar_dbconf_results
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
        return HttpResponse(JSONUtil.dumps(data_package), content_type='application/json')

    revs = int(request.GET['revs'])

    # Get all results related to the selected DBMS, sort by time
    results = Result.objects.filter(application=request.GET['proj'])
    results = filter(lambda x: x.dbms.key in request.GET['db'].split(','), results)
    results = sorted(results, cmp=lambda x, y: int((x.timestamp - y.timestamp).total_seconds()))
    # Determine which benchmark is selected

    table_results = []
#     if request.GET['ben'] == 'grid':
#         table_results = results
    if request.GET['ben'] == 'show_none':
        pass
    else:
#         benchmarks = []
        
        if request.GET['ben'] == 'grid':
            benchmarks = set()
            benchmark_confs = []
            for result in results:
                benchmarks.add(result.benchmark_config.benchmark_type)
                benchmark_confs.append(result.benchmark_config)
        else: 
            benchmarks = [request.GET['ben']]
            benchmark_confs = filter(lambda x: x != '', request.GET['spe'].strip().split(','))
            results = filter(lambda x: str(x.benchmark_config.pk) in benchmark_confs, results)

        for f in filter(lambda x: x != '', request.GET.getlist('add[]', [])):
            key, value = f.split(':')
            if value == 'select_all':
                continue
            results = filter(lambda x: getattr(x.benchmark_config, key) == value, results)

        table_results = results

        if len(benchmarks) == 1:
            metrics = request.GET.get('met', 'throughput,p99_latency').split(',')
        else:
            metrics = ['throughput', 'p99_latency']

    # For the data table
    data_package['results'] = [
        [x.pk,
         x.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
         x.dbms_config.name,
         x.dbms_metrics.name,
         x.benchmark_config.name,
         x.throughput * METRIC_META['throughput']['scale'],
         x.p99_latency * METRIC_META['p99_latency']['scale'],
         x.dbms_config.pk,
         x.dbms_metrics.pk,
         x.benchmark_config.pk
        ]
        for x in table_results]

    # For plotting charts
    for metric in metrics:
        for bench in benchmarks:
            b_r = filter(lambda x: x.benchmark_config.benchmark_type == bench, results)
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
                d_r = filter(lambda x: x.dbms.key == db, b_r)
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

    return HttpResponse(JSONUtil.dumps(data_package), content_type='application/json')
