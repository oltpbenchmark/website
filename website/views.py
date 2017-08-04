import logging
import string
from collections import OrderedDict
from pytz import timezone, os
from random import choice
from rexec import FileWrapper

import xml.dom.minidom
from django.contrib.auth import login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import AuthenticationForm, UserCreationForm
from django.core.cache import cache
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Q
from django.http import HttpResponse, QueryDict, Http404
from django.shortcuts import redirect, render, get_object_or_404
from django.template.context_processors import csrf
from django.template.defaultfilters import register
from django.utils.datetime_safe import datetime
from django.utils.timezone import now
from django.views.decorators.csrf import csrf_exempt
from djcelery.models import TaskMeta

from .forms import NewResultForm, TuningSessionCheckbox
from .models import (Application, BenchmarkConfig, DBConf, DBMSCatalog,
                     DBMSMetrics, Hardware, KnobCatalog, MetricCatalog,
                     Project, Result, ResultData, Statistics, Task,
                     WorkloadCluster, METRIC_META, PLOTTABLE_FIELDS)
from .settings import UPLOAD_DIR
# from tasks import run_gpr, run_wm, preprocess, process_result_data
from .types import DBMSType, HardwareType, MetricType, TaskType
from .utils import JSONUtil

log = logging.getLogger(__name__)


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
                data[metric].append(
                    [t.time - offset,
                        getattr(t, metric) * METRIC_META[metric]['scale']])
    return HttpResponse(JSONUtil.dumps(data), content_type='application/json')


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
            log.info("Invalid request: {}".format(
                ', '.join(form.error_messages)))

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
            log.info("Invalid request: {}".format(
                ', '.join(form.error_messages)))
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


def upload_code_generator(size=6,
                          chars=string.ascii_uppercase + string.digits):
    # We must make sure this code does not already exist in the database
    # although duplicates should be extremely rare.
    new_upload_code = ''.join(choice(chars) for _ in range(size))
    num_dup_codes = Project.objects.filter(upload_code=new_upload_code).count()
    while (num_dup_codes > 0):
        new_upload_code = ''.join(choice(chars) for _ in range(size))
        num_dup_codes = Project.objects.filter(
            upload_code=new_upload_code).count()
    return new_upload_code


@login_required(login_url='/login/')
def home(request):
    context = {"projects": Project.objects.filter(user=request.user)}
    context.update(csrf(request))
    return render(request, 'home.html', context)


@login_required(login_url='/login/')
def ml_info(request):
    result_id = request.GET['id']
    res = Result.objects.get(pk=result_id)
    tasks = Task.objects.filter(result=res)
    assert len(tasks) == 3
    tasks = sorted(tasks, cmp=lambda x, y: x.type < y.type)
    task_metas = TaskMeta.objects.filter(reduce(
            lambda x, y: x | y, [Q(task_id=obj.taskmeta_id) for obj in tasks]))
    if len(task_metas) == 0:
        raise Http404()

    task_info = OrderedDict()
    for task in tasks:
        if task.start_time is None:
            raise Http404()
        task_dict = {}
        task_meta = filter(lambda x: x.task_id == task.taskmeta_id, task_metas)
        if len(task_meta) == 0:
            continue
        else:
            assert len(task_meta) == 1
            task_meta = task_meta[0]

        task_dict['runtime'] = (task_meta.date_done - task.start_time).seconds
        task_dict.update(task.__dict__)
        task_dict.update(task_meta.__dict__)
        task_info[TaskType.TYPE_NAMES[task.type]] = task_dict

    overall_status = 'SUCCESS'
    num_completed = 0
    for entry in task_info.values():
        status = entry['status']
        if status == "SUCCESS":
            num_completed += 1
        elif status in ['FAILURE', 'REVOKED', 'RETRY']:
            overall_status = status
            break
        else:
            assert status in ['PENDING', 'RECEIVED', 'STARTED']
            overall_status = status
    context = {"id": result_id,
               "result": res,
               "overall_status": overall_status,
               "num_completed": "{} / {}".format(num_completed, len(tasks)),
               "tasks": task_info,
               "limit": "300"}

    return render(request, "ml_info.html", context)


@login_required(login_url='/login/')
def project(request):
    project_id = request.GET['id']
    applications = Application.objects.filter(project=project_id)
    project = Project.objects.get(pk=project_id)
    context = {"applications": applications,
               "project": project,
               "proj_id": project_id}
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
    dbs = {}
    benchmarks = {}

    for res in results:
        dbs[res.dbms.key] = res.dbms
        bench_type = res.benchmark_config.benchmark_type
        if bench_type not in benchmarks:
            benchmarks[bench_type] = set()
        benchmarks[bench_type].add(res.benchmark_config)

    benchmarks = {k: sorted(list(v)) for k, v in benchmarks.iteritems()}
    benchmarks = OrderedDict(sorted(benchmarks.iteritems()))

    lastrevisions = [10, 50, 100]
    dbs = OrderedDict(sorted(dbs.items()))
    filters = []
#     for field in BenchmarkConfig.FILTER_FIELDS:
#         value_dict = {}
#         for res in results:
#             value_dict[getattr(res.benchmark_conf, field['field'])] = True
#         f = {'values': [key for key in value_dict.iterkeys()],
#              'print': field['print'], 'field': field['field']}
#         filters.append(f)
    defaultspe = "none" if len(benchmarks) == 0 else \
        list(benchmarks.iteritems())[0][0]
    context = {'project': project,
               'dbmss': dbs,
               'benchmarks': benchmarks,
               'lastrevisions': lastrevisions,
               'defaultdbms': "none" if len(dbs) == 0 else dbs.keys()[0],
               'defaultlast': 10,
               'defaultequid': False,
               'defaultbenchmark': 'grid',
               'defaultspe': defaultspe,
               'metrics': PLOTTABLE_FIELDS,
               'metric_meta': METRIC_META,
               'defaultmetrics': ['throughput', 'p99_latency'],
               'filters': filters,
               'application': application,
               'results': results}

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
    context['form'] = TuningSessionCheckbox(request.POST or None)
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
    return redirect('/project/?id=' + request.POST['id'])


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
    else:
        p = Project.objects.get(pk=proj_id)
        if p.user != request.user:
            return render(request, '404.html')

    if gen_upload_code:
        p.upload_code = upload_code_generator(size=20)

    p.name = request.POST['name']
    p.description = request.POST['description']
    p.last_update = now()
    p.save()
    applications = Application.objects.filter(project=p)

    context = {'project': p,
               'proj_id': p.pk,
               'applications': applications}
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
        # FIXME (dva): hardware type is hardcoded for now
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

    p.name = request.POST['name']
    p.description = request.POST['description']
    p.last_update = now()
    tuning_form = TuningSessionCheckbox(request.POST or None)
    if tuning_form.is_valid():
        p.tuning_session = tuning_form.cleaned_data['tuning_session']
    p.save()
    return redirect('/application/?id=' + str(p.pk))


def write_file(contents, output_file, chunk_size=512):
    des = open(output_file, 'w')
    for chunk in contents.chunks():
        des.write(chunk)
    des.close()


@csrf_exempt
def new_result(request):
    if request.method == 'POST':
        form = NewResultForm(request.POST, request.FILES)

        if not form.is_valid():
            log.warning("Form is not valid:\n" + str(form))
            return HttpResponse("Form is not valid\n" + str(form))
        upload_code = form.cleaned_data['upload_code']
        cluster_name = form.cleaned_data['cluster_name']
        try:
            application = Application.objects.get(upload_code=upload_code)
        except Application.DoesNotExist:
            log.warning("Wrong upload code: " + upload_code)
            return HttpResponse("wrong upload_code!")

        return handle_result_files(application, request.FILES, cluster_name)
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


def handle_result_files(app, files, cluster_name):
    from .utils import DBMSUtil
    from celery import chain

    # Load summary file
    summary = JSONUtil.loads(''.join(files['summary_data'].chunks()))

    # Verify that the database/version is supported
    dbms_type = DBMSType.type(summary['DBMS Type'])
    # FIXME! bad hack until I have time to get the PG 9.3 metric/knob data in
    # the same form
    dbms_version = "9.6"
#     dbms_version = DBMSUtil.parse_version_string(
#        dbms_type, summary['DBMS Version'])

    try:
        dbms_object = DBMSCatalog.objects.get(
            type=dbms_type, version=dbms_version)
    except ObjectDoesNotExist:
        return HttpResponse('{} v{} is not yet supported.'.format(
            summary['DBMS Type'], dbms_version))

    # Load DB parameters file
    db_parameters = JSONUtil.loads(
        ''.join(files['db_parameters_data'].chunks()))

    # Load DB metrics file
    db_metrics = JSONUtil.loads(''.join(files['db_metrics_data'].chunks()))

    # Load benchmark config file
    benchmark_config_str = ''.join(files['benchmark_conf_data'].chunks())

    # Load samples file
    samples = ''.join(files['sample_data'].chunks())

    benchmark_configs = BenchmarkConfig.objects.filter(
        configuration=benchmark_config_str)
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
        benchmark_config.isolation = (root.getElementsByTagName('isolation'))[
            0].firstChild.data
        benchmark_config.scalefactor = (
            root.getElementsByTagName('scalefactor'))[0].firstChild.data
        benchmark_config.terminals = (root.getElementsByTagName('terminals'))[
            0].firstChild.data
        benchmark_config.time = (root.getElementsByTagName('time'))[
            0].firstChild.data
        benchmark_config.rate = (root.getElementsByTagName('rate'))[
            0].firstChild.data
        benchmark_config.skew = (root.getElementsByTagName('skew'))
        benchmark_config.skew = - \
            1 if len(benchmark_config.skew) == 0 else benchmark_config.skew[
                0].firstChild.data
        benchmark_config.transaction_types = [
            t.firstChild.data for t in root.getElementsByTagName('name')]
        benchmark_config.transaction_weights = [
            w.firstChild.data for w in root.getElementsByTagName('weights')]
        benchmark_config.save()
        benchmark_config.name = benchmark_config.benchmark_type + '@' + \
            benchmark_config.creation_time.strftime("%Y-%m-%d,%H") + \
            '#' + str(benchmark_config.pk)
        benchmark_config.save()

    knob_catalog = KnobCatalog.objects.filter(dbms=dbms_object)
    db_conf_dict, db_diffs = DBMSUtil.parse_dbms_config(dbms_object.type,
                                                        db_parameters,
                                                        knob_catalog)
    db_conf_str = JSONUtil.dumps(db_conf_dict, pprint=True, sort=True)

    creation_time = now()
    db_confs = DBConf.objects.filter(
        configuration=db_conf_str, application=app)
    if len(db_confs) >= 1:
        db_conf = db_confs[0]
    else:
        db_conf = DBConf()
        db_conf.creation_time = creation_time
        db_conf.name = ''
        db_conf.configuration = db_conf_str
        db_conf.orig_config_diffs = JSONUtil.dumps(db_diffs, pprint=True)
        db_conf.application = app
        db_conf.dbms = dbms_object
        db_conf.description = ''
        db_conf.save()
        db_conf.name = dbms_object.key + '@' + \
            creation_time.strftime("%Y-%m-%d,%H") + '#' + str(db_conf.pk)
        db_conf.save()

    db_metrics_catalog = MetricCatalog.objects.filter(dbms=dbms_object)
    db_metrics_dict, met_diffs = DBMSUtil.parse_dbms_metrics(
            dbms_object.type, db_metrics, db_metrics_catalog)
    dbms_metrics = DBMSMetrics()
    dbms_metrics.creation_time = creation_time
    dbms_metrics.name = ''
    dbms_metrics.configuration = JSONUtil.dumps(
        db_metrics_dict, pprint=True, sort=True)
    dbms_metrics.orig_config_diffs = JSONUtil.dumps(met_diffs, pprint=True)
    dbms_metrics.execution_time = benchmark_config.time
    dbms_metrics.application = app
    dbms_metrics.dbms = dbms_object
    dbms_metrics.save()
    dbms_metrics.name = dbms_object.key + '@' + \
        creation_time.strftime("%Y-%m-%d,%H") + '#' + str(dbms_metrics.pk)
    dbms_metrics.save()

    result = Result()
    result.application = app
    result.dbms = dbms_object
    result.dbms_config = db_conf
    result.dbms_metrics = dbms_metrics
    result.benchmark_config = benchmark_config

    result.summary = JSONUtil.dumps(summary, pprint=True, sort=True)
    result.samples = samples

    result.timestamp = datetime.fromtimestamp(
        int(summary['Current Timestamp (milliseconds)']) / 1000,
        timezone("UTC"))
    result.hardware = app.hardware

    latencies = {k: float(l) for k, l in summary[
        'Latency Distribution'].iteritems()}
    result.avg_latency = latencies['Average Latency (microseconds)']
    result.min_latency = latencies['Minimum Latency (microseconds)']
    result.p25_latency = latencies['25th Percentile Latency (microseconds)']
    result.p50_latency = latencies['Median Latency (microseconds)']
    result.p75_latency = latencies['75th Percentile Latency (microseconds)']
    result.p90_latency = latencies['90th Percentile Latency (microseconds)']
    result.p95_latency = latencies['95th Percentile Latency (microseconds)']
    result.p99_latency = latencies['99th Percentile Latency (microseconds)']
    result.max_latency = latencies['Maximum Latency (microseconds)']
    result.throughput = float(summary['Throughput (requests/second)'])
    result.creation_time = now()
    result.save()

    sample_lines = samples.split('\n')
    header = [h.strip() for h in sample_lines[0].split(',')]

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

    if cluster_name is not None:
        try:
            wkld_cluster = WorkloadCluster.objects.get(
                dbms=dbms_object,
                hardware=app.hardware,
                cluster_name=cluster_name)
        except WorkloadCluster.DoesNotExist:
            wkld_cluster = WorkloadCluster()
            wkld_cluster.dbms = dbms_object
            wkld_cluster.hardware = app.hardware
            wkld_cluster.cluster_name = cluster_name
            wkld_cluster.save()
    else:
        wkld_cluster = WorkloadCluster.get_default_cluster(
            dbms_object, app.hardware)

    tunable_param_catalog = filter(lambda x: x.tunable is True, knob_catalog)
    tunable_params = {p.name: db_conf_dict[p.name]
                      for p in tunable_param_catalog}
    param_data = DBMSUtil.preprocess_dbms_params(
        dbms_object.type, tunable_params, tunable_param_catalog)

    numeric_metric_catalog = filter(
        lambda x: x.metric_type != MetricType.INFO, db_metrics_catalog)
    numeric_metrics = {p.name: db_metrics_dict[
        p.name] for p in numeric_metric_catalog}

    external_metrics = dict(summary['Latency Distribution'])
    external_metrics['Throughput (requests/second)'] = summary[
        'Throughput (requests/second)']
    metric_data = DBMSUtil.preprocess_dbms_metrics(dbms_type,
                                                   numeric_metrics,
                                                   numeric_metric_catalog,
                                                   external_metrics,
                                                   int(benchmark_config.time))

    res_data = ResultData()
    res_data.result = result
    res_data.cluster = wkld_cluster
    res_data.param_data = JSONUtil.dumps(param_data, pprint=True, sort=True)
    res_data.metric_data = JSONUtil.dumps(metric_data, pprint=True, sort=True)
    res_data.save()

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

    if app.tuning_session is False:
        return HttpResponse("Store Success !")

#     response = chain(preprocess.s(1, 2), run_wm.s(3), run_gpr.s(4)).apply_async()
#     taskmeta_ids = [response.parent.parent.id, response.parent.id, response.id]
#     task_ids = []
#     for i, tmid in enumerate(taskmeta_ids):
#         task = Task()
#         task.taskmeta_id = tmid
#         task.start_time = None
#         task.result = result
#         task.type = TaskType.TYPE_NAMES.keys()[i]
#         task.save()
#         task_ids.append(str(task.pk))
#     result.task_ids = ','.join(task_ids)
#     result.save()
#     response = process_result_data.delay(PipelineResult.get_newest_version())
    return HttpResponse("Store Success ! Running tuner... (status={})")


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

    def combine_dicts(d1, d2):
        d3 = dict(d1)
        d3.update(d2)
        return OrderedDict(sorted(d3.iteritems()))

    dbms_metrics = get_object_or_404(DBMSMetrics, pk=request.GET['id'])
    if dbms_metrics.application.user != request.user:
        raise Http404()
    numeric_dict, other_dict = dbms_metrics.get_numeric_configuration(
        normalize=True, return_both=True)
    metric_dict = combine_dicts(numeric_dict, other_dict)

    if 'compare' in request.GET and request.GET['compare'] != 'none':
        compare_obj = DBMSMetrics.objects.get(pk=request.GET['compare'])
        comp_numeric_dict, comp_other_dict = compare_obj.get_numeric_configuration(
            normalize=True, return_both=True)
        comp_dict = combine_dicts(comp_numeric_dict, comp_other_dict)

        metrics = [(k, v, comp_dict[k]) for k, v in metric_dict.iteritems()]
        numeric_metrics = [(k, v, comp_numeric_dict[k])
                           for k, v in numeric_dict.iteritems()]
    else:
        metrics = list(metric_dict.iteritems())
        numeric_metrics = list(metric_dict.iteritems())

    peer_metrics = DBMSMetrics.objects.filter(
        dbms=dbms_metrics.dbms, application=dbms_metrics.application)
    peer_metrics = filter(lambda x: x.pk != dbms_metrics.pk, peer_metrics)

    context = {'metrics': metrics,
               'numeric_metrics': numeric_metrics,
               'dbms_metrics': dbms_metrics,
               'compare': request.GET.get('compare', 'none'),
               'peer_dbms_metrics': peer_metrics}
    return render(request, 'dbms_metrics.html', context)


@login_required(login_url='/login/')
def db_conf_view(request):

    def combine_dicts(d1, d2):
        d3 = dict(d1)
        d3.update(d2)
        return OrderedDict(sorted(d3.iteritems()))

    db_conf = get_object_or_404(DBConf, pk=request.GET['id'])
    if db_conf.application.user != request.user:
        raise Http404()
    tuning_dict, other_dict = db_conf.get_tuning_configuration(
        return_both=True)
    params_dict = combine_dicts(tuning_dict, other_dict)

    if 'compare' in request.GET and request.GET['compare'] != 'none':
        compare_obj = DBConf.objects.get(pk=request.GET['compare'])
        comp_tuning_dict, comp_other_dict = compare_obj.get_tuning_configuration(
            return_both=True)
        comp_dict = combine_dicts(comp_tuning_dict, comp_other_dict)

        params = [(k, v, comp_dict[k]) for k, v in params_dict.iteritems()]
        tuning_params = [(k, v, comp_tuning_dict[k])
                         for k, v in tuning_dict.iteritems()]
    else:
        params = list(params_dict.iteritems())
        tuning_params = list(tuning_dict.iteritems())
    peer_params = DBConf.objects.filter(
        dbms=db_conf.dbms, application=db_conf.application)
    peer_params = filter(lambda x: x.pk != db_conf.pk, peer_params)

    context = {'parameters': params,
               'featured_par': tuning_params,
               'db_conf': db_conf,
               'compare': request.GET.get('compare', 'none'),
               'peer_db_conf': peer_params}
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

        db_confs = DBConf.objects.filter(
            application=benchmark_conf.application, dbms=dbms_object)
        for db_conf in db_confs:
            rs = Result.objects.filter(
                dbms_config=db_conf, benchmark_config=benchmark_conf)
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
    results = sorted(results, cmp=lambda x,
                     y: int(y.throughput - x.throughput))

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
                [i, getattr(r, met) * METRIC_META[met]['scale'],
                 r.pk, getattr(r, met) * METRIC_META[met]['scale']])
            data_package['results'][-1]['tick'].append(r.dbms_config.name)
            i -= 1
        data_package['results'][-1]['data'].reverse()
        data_package['results'][-1]['tick'].reverse()

    return HttpResponse(JSONUtil.dumps(data_package), content_type='application/json')


@login_required(login_url='/login/')
def get_benchmark_conf_file(request):
    bench_id = request.GET['id']
    benchmark_conf = get_object_or_404(BenchmarkConfig, pk=request.GET['id'])
    if benchmark_conf.application.user != request.user:
        return render(request, '404.html')

    response = HttpResponse(benchmark_conf.configuration,
                            content_type='text/plain')
    response['Content-Disposition'] = 'attachment; filename=result_' + \
        str(bench_id) + '.ben.cnf'
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
    db_conf_a = a.dbms_config.get_tuning_configuration()
    db_conf_b = b.dbms_config.get_tuning_configuration()
    for k, v in db_conf_a.iteritems():
        if k not in db_conf_b or v != db_conf_b[k]:
            return False
    return True


def result_same(a, b):
    db_conf_a = JSONUtil.loads(a.dbms_config.configuration)
    db_conf_b = JSONUtil.loads(b.dbms_config.configuration)
    for k, v in db_conf_a.iteritems():
        if k not in db_conf_b or v != db_conf_b[k]:
            return False
    return True


@login_required(login_url='/login/')
def update_similar(request):
    raise Http404()


def result(request):
    target = get_object_or_404(Result, pk=request.GET['id'])
    data_package = {}
    results = Result.objects.filter(application=target.application,
                                    dbms=target.dbms,
                                    benchmark_config=target.benchmark_config)
    same_dbconf_results = filter(lambda x: result_same(
        x, target) and x.pk != target.pk, results)
    similar_dbconf_results = filter(lambda x: result_similar(x, target) and
                                    x.pk not in ([target.pk] +
                                    [r.pk for r in same_dbconf_results]), results)
    less_is_better = METRIC_META[metric]['lessisbetter'] and \
        '(less is better)' or '(more is better)'
    for metric in PLOTTABLE_FIELDS:
        data_package[metric] = {
            'data': {},
            'units': METRIC_META[metric]['unit'],
            'lessisbetter': less_is_better,
            'metric': METRIC_META[metric]['print']
        }

        same_id = []
        same_id.append(str(target.pk))
        for x in same_id:
            key = metric + ',data,' + x
            tmp = cache.get(key)
            if tmp is not None:
                data_package[metric]['data'][int(x)] = []
                data_package[metric]['data'][int(x)].extend(tmp)
                continue

            ts = Statistics.objects.filter(result=x)
            if len(ts) > 0:
                offset = ts[0].time
                if len(ts) > 1:
                    offset -= ts[1].time - ts[0].time
                data_package[metric]['data'][int(x)] = []
                for t in ts:
                    data_package[metric]['data'][int(x)].append(
                        [t.time - offset, getattr(t, metric) * METRIC_META[metric]['scale']])
                cache.set(key, data_package[metric]['data'][int(x)], 60 * 5)

    default_metrics = {}
    for met in ['throughput', 'p99_latency']:
        default_metrics[met] = '{0:0.2f}'.format(
            getattr(target, met) * METRIC_META[met]['scale'])

    status = None
    if target.task_ids is not None:
        tasks = Task.objects.filter(result=target)
        for task in tasks[::-1]:
            tm = TaskMeta.objects.filter(task_id=task.taskmeta_id).first()
            if tm is not None:
                status = tm.status
                break
        if status is None:
            status = 'UNKNOWN'

    context = {
        'result': target,
        'metrics': PLOTTABLE_FIELDS,
        'metric_meta': METRIC_META,
        'default_metrics': default_metrics,
        'data': JSONUtil.dumps(data_package),
        'same_runs': same_dbconf_results,
        'status': status,
        'similar_runs': similar_dbconf_results
    }
    return render(request, 'result.html', context)


@login_required(login_url='/login/')
def get_result_data_file(request):
    target = get_object_or_404(Result, pk=request.GET['id'])
    # task =  get_object_or_404(Task, pk=request.GET['id'])
    if target.application.user != request.user:
        return render(request, '404.html')

    result_id = int(request.GET['id'])
    result_type = request.GET['type']

    prefix = get_result_data_dir(result_id)

    if result_type == 'sample':
        response = HttpResponse(FileWrapper(
            file(prefix + '_' + result_type)), content_type='text/plain')
        response.__setitem__(
            'Content-Disposition', 'attachment; filename=result_' + str(result_id) + '.sample')
        return response
    elif result_type == 'raw':
        response = HttpResponse(FileWrapper(
            file(prefix + '_' + result_type)), content_type='text/plain')
        response['Content-Disposition'] = 'attachment; filename=result_' + \
            str(result_id) + '.raw'
        return response
    elif result_type == 'new_conf':
        response = HttpResponse(FileWrapper(
            file(prefix + '_' + result_type)), content_type='text/plain')
        response['Content-Disposition'] = 'attachment; filename=result_' + \
            str(result_id) + '_new_conf'
        return response


# Data Format:
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
    results = filter(lambda x: x.dbms.key in request.GET[
                     'db'].split(','), results)
    results = sorted(results, cmp=lambda x, y: int(
        (x.timestamp - y.timestamp).total_seconds()))

    table_results = []
    if request.GET['ben'] == 'show_none':
        pass
    else:

        if request.GET['ben'] == 'grid':
            benchmarks = set()
            benchmark_confs = []
            for result in results:
                benchmarks.add(result.benchmark_config.benchmark_type)
                benchmark_confs.append(result.benchmark_config)
        else:
            benchmarks = [request.GET['ben']]
            benchmark_confs = filter(lambda x: x != '', request.GET[
                                     'spe'].strip().split(','))
            results = filter(lambda x: str(x.benchmark_config.pk)
                             in benchmark_confs, results)

        for f in filter(lambda x: x != '', request.GET.getlist('add[]', [])):
            key, value = f.split(':')
            if value == 'select_all':
                continue
            results = filter(lambda x: getattr(
                x.benchmark_config, key) == value, results)

        table_results = results

        if len(benchmarks) == 1:
            metrics = request.GET.get(
                'met', 'throughput,p99_latency').split(',')
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
            b_r = filter(
                lambda x: x.benchmark_config.benchmark_type == bench, results)
            if len(b_r) == 0:
                continue

            less_is_better = METRIC_META[metric]['lessisbetter'] and \
                '(less is better)' or '(more is better)'
            data = {
                'benchmark': bench,
                'units': METRIC_META[metric]['unit'],
                'lessisbetter': less_is_better,
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
