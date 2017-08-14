import logging

from collections import OrderedDict
from pytz import timezone

from django.contrib.auth import login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import AuthenticationForm, UserCreationForm
from django.core.cache import cache
from django.core.exceptions import ObjectDoesNotExist
from django.http import HttpResponse, QueryDict, Http404
from django.shortcuts import redirect, render, get_object_or_404
from django.template.context_processors import csrf
from django.template.defaultfilters import register
from django.utils.datetime_safe import datetime
from django.utils.timezone import now
from django.views.decorators.csrf import csrf_exempt
from djcelery.models import TaskMeta

from .forms import ApplicationForm, NewResultForm
from .models import (Application, BenchmarkConfig, DBConf, DBMSCatalog,
                     DBMSMetrics, Project, Result, ResultData,
                     Statistics, WorkloadCluster)
from tasks import aggregate_target_results, map_workload, configuration_recommendation
from .types import DBMSType, StatsType, TaskType
from .utils import DBMSUtil, JSONUtil, MediaUtil

log = logging.getLogger(__name__)


# For the html template to access dict object
@register.filter
def get_item(dictionary, key):
    return dictionary.get(key)


def ajax_new(request):
    new_id = request.GET['new_id']
    ts = Statistics.objects.filter(result=new_id)
    data = {}
    metric_meta = Statistics.objects.METRIC_META
    for metric, metric_info in metric_meta.iteritems():
        if len(ts) > 0:
            offset = ts[0].time
            if len(ts) > 1:
                offset -= ts[1].time - ts[0].time
            data[metric] = []
            for t in ts:
                data[metric].append(
                    [t.time - offset,
                        getattr(t, metric) * metric_info['scale']])
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


@login_required(login_url='/login/')
def home(request):
    context = {"projects": Project.objects.filter(user=request.user)}
    context.update(csrf(request))
    return render(request, 'home.html', context)


def get_task_status(tasks):
    if len(tasks) == 0:
        return None, 0
    overall_status = 'SUCCESS'
    num_completed = 0
    for task in tasks:
        status = task.status
        if status == "SUCCESS":
            num_completed += 1
        elif status in ['FAILURE', 'REVOKED', 'RETRY']:
            overall_status = status
            break
        else:
            assert status in ['PENDING', 'RECEIVED', 'STARTED']
            overall_status = status
    return overall_status, num_completed


@login_required(login_url='/login/')
def ml_info(request):
    result_id = request.GET['id']
    res = Result.objects.get(pk=result_id)

    task_ids = res.task_ids.split(',')
    tasks = []
    for tid in task_ids:
        task = TaskMeta.objects.filter(task_id=tid).first()
        if task is not None:
            tasks.append(task)

    overall_status, num_completed = get_task_status(tasks)
    if overall_status in ['PENDING', 'RECEIVED', 'STARTED']:
        completion_time = 'N/A'
        total_runtime = 'N/A'
    else:
        completion_time = tasks[-1].date_done
        total_runtime = (completion_time - res.creation_time).total_seconds()
        total_runtime = '{0:.2f} seconds'.format(total_runtime)

    task_info = [(tname, task) for tname, task in \
                 zip(TaskType.TYPE_NAMES.values(), tasks)]

    context = {"id": result_id,
               "result": res,
               "overall_status": overall_status,
               "num_completed": "{} / {}".format(num_completed, 3),
               "completion_time": completion_time,
               "total_runtime": total_runtime,
               "tasks": task_info}

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

    metric_meta = Statistics.objects.METRIC_META
    context = {'project': project,
               'dbmss': dbs,
               'benchmarks': benchmarks,
               'lastrevisions': lastrevisions,
               'defaultdbms': "none" if len(dbs) == 0 else dbs.keys()[0],
               'defaultlast': 10,
               'defaultequid': False,
               'defaultbenchmark': 'grid',
               'defaultspe': defaultspe,
               'metrics': metric_meta.keys(),
               'metric_meta': metric_meta,
               'defaultmetrics': Statistics.objects.DEFAULT_METRICS,
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


# @login_required(login_url='/login/')
# def edit_application(request):
#     project = get_object_or_404(Project, pk=request.GET['pid'])
#     if project.user != request.user:
#         return Http404()
#     if request.GET['id'] != '':
#         app = Application.objects.get(pk=int(request.GET['id']))
#         form = ApplicationForm(instance=app)
# #         form.fields['dbms'].widget.attrs['disabled'] = True
# #         form.fields['dbms'].widget.attrs['readonly'] = True
# #         form.fields['hardware'].widget.attrs['disabled'] = True
# #         form.fields['hardware'].widget.attrs['readonly'] = True
#     else:
#         app = None
#         form = ApplicationForm()
#     context = {
#         'project': project,
#         'application': app,
#         'form': form,
#     }
#     return render(request, 'edit_application.html', context)


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
        p.upload_code = MediaUtil.upload_code_generator(size=20)

    p.name = request.POST['name']
    p.description = request.POST['description']
    p.last_update = now()
    p.save()
    applications = Application.objects.filter(project=p)

    context = {'project': p,
               'proj_id': p.pk,
               'applications': applications}
    return render(request, 'home_application.html', context)

@login_required(login_url='/login/')
def update_application(request):
    if request.method == 'POST':
        app_id, proj_id = request.POST['id'].split('&')
        project = Project.objects.get(pk=int(proj_id))
        if project.user != request.user:
            return Http404()
        if app_id == '':
            form = ApplicationForm(request.POST)
            if not form.is_valid():
                return HttpResponse(str(form))
            app = form.save(commit=False)
            app.user = request.user
            app.project = project
            ts = now()
            app.creation_time = ts
            app.last_update = ts
            app.upload_code = MediaUtil.upload_code_generator()
            app.save()
        else:
            app = Application.objects.get(pk=int(app_id))
            form = ApplicationForm(request.POST, instance=app)
            if not form.is_valid():
                return HttpResponse(str(form))
            if form.cleaned_data['gen_upload_code'] is True:
                app.upload_code = MediaUtil.upload_code_generator()
            app.last_update = now()
            app.save()
        return redirect('/application/?id=' + str(app.pk))
    else:
        project = get_object_or_404(Project, pk=request.GET['pid'])
        if project.user != request.user:
            return Http404()
        if request.GET['id'] != '':
            app = Application.objects.get(pk=int(request.GET['id']))
            form = ApplicationForm(instance=app)
#             form.fields['dbms'].widget.attrs['disabled'] = True
#             form.fields['dbms'].widget.attrs['readonly'] = True
#             form.fields['hardware'].widget.attrs['disabled'] = True
#             form.fields['hardware'].widget.attrs['readonly'] = True
        else:
            app = None
            form = ApplicationForm()
        context = {
            'project': project,
            'application': app,
            'form': form,
        }
        return render(request, 'edit_application.html', context)


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


def handle_result_files(app, files, cluster_name):
    from celery import chain

    print 'FILES TYPE: {}'.format(type(files))
    print 'FILE TYPE: {}'.format(type(files['summary_data']))
    # Load summary file and verify that the database/version is supported
    summary = JSONUtil.loads(''.join(files['summary_data'].chunks()))
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

    # Load parameters, metrics, benchmark, and samples
    db_parameters = JSONUtil.loads(
        ''.join(files['db_parameters_data'].chunks()))
    db_metrics = JSONUtil.loads(''.join(files['db_metrics_data'].chunks()))
    benchmark_config_str = ''.join(files['benchmark_conf_data'].chunks())
    samples = ''.join(files['sample_data'].chunks())

    benchmark_config = BenchmarkConfig.objects.create_benchmark_config(
        app, benchmark_config_str, summary['Benchmark Type'].upper())

    db_conf_dict, db_diffs = DBMSUtil.parse_dbms_config(
        dbms_object.pk, db_parameters)
    db_conf = DBConf.objects.create_dbconf(
        app, JSONUtil.dumps(db_conf_dict, pprint=True, sort=True),
        JSONUtil.dumps(db_diffs), dbms_object)

    db_metrics_dict, met_diffs = DBMSUtil.parse_dbms_metrics(
            dbms_object.pk, db_metrics)
    dbms_metrics = DBMSMetrics.objects.create_dbms_metrics(
        app, JSONUtil.dumps(db_metrics_dict, pprint=True, sort=True),
        JSONUtil.dumps(met_diffs), benchmark_config.time, dbms_object)

    timestamp = datetime.fromtimestamp(
        int(summary['Current Timestamp (milliseconds)']) / 1000,
        timezone("UTC"))
    result = Result.objects.create_result(
        app, dbms_object, benchmark_config, db_conf, dbms_metrics,
        JSONUtil.dumps(summary, pprint=True, sort=True), samples,
        timestamp)
    result.summary_stats = Statistics.objects.create_summary_stats(
        summary, result, benchmark_config.time)
    result.save()
    Statistics.objects.create_sample_stats(samples, result)

    wkld_cluster = WorkloadCluster.objects.create_workload_cluster(
        dbms_object, app.hardware, cluster_name)
    param_data = DBMSUtil.convert_dbms_params(
        dbms_object.pk, db_conf_dict)
    external_metrics = Statistics.objects.get_external_metrics(summary)
    metric_data = DBMSUtil.convert_dbms_metrics(
        dbms_object.pk, db_metrics_dict, external_metrics,
        int(benchmark_config.time))

    ResultData.objects.create_result_data(
        result, wkld_cluster, JSONUtil.dumps(param_data, pprint=True, sort=True),
        JSONUtil.dumps(metric_data, pprint=True, sort=True))

    nondefault_settings = DBMSUtil.get_nondefault_settings(dbms_object.pk,
                                                           db_conf_dict)
    app.project.last_update = now()
    app.last_update = now()
    if app.nondefault_settings is None:
        app.nondefault_settings = JSONUtil.dumps(nondefault_settings)
    app.project.save()
    app.save()

    path_prefix = MediaUtil.get_result_data_path(result.pk)
    paths = [
        (path_prefix + '.samples', 'sample_data'),
        (path_prefix + '.summary', 'summary_data'),
        (path_prefix + '.params', 'db_parameters_data'),
        (path_prefix + '.metrics', 'db_metrics_data'),
        (path_prefix + '.expconfig', 'benchmark_conf_data'),
    ]

    for path, content_name in paths:
        with open(path, 'w') as f:
            for chunk in files[content_name].chunks():
                f.write(chunk)

    if 'raw_data' in files:
        with open('{}.csv.tgz'.format(path_prefix), 'w') as f:
            for chunk in files['raw_data'].chunks():
                f.write(chunk)

    if app.tuning_session is False:
        return HttpResponse("Store success!")

    response = chain(aggregate_target_results.s(result.pk),
                     map_workload.s(),
                     configuration_recommendation.s()).apply_async()
    taskmeta_ids = [response.parent.parent.id, response.parent.id, response.id]
    result.task_ids = ','.join(taskmeta_ids)
    result.save()
    return HttpResponse("Store Success! Running tuner... (status={})".format(
        response.status))


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

    dbms_id = dbms_metrics.dbms.pk
    metric_dict = JSONUtil.loads(dbms_metrics.configuration)
    numeric_dict = DBMSUtil.filter_numeric_metrics(
        dbms_id, metric_dict, normalize=True)
#     numeric_dict, other_dict = dbms_metrics.get_numeric_configuration(
#         normalize=True, return_both=True)
#     metric_dict = combine_dicts(numeric_dict, other_dict)

    if 'compare' in request.GET and request.GET['compare'] != 'none':
        compare_obj = DBMSMetrics.objects.get(pk=request.GET['compare'])
        comp_dict = JSONUtil.loads(compare_obj.configuration)
        comp_numeric_dict = DBMSUtil.filter_numeric_metrics(
            dbms_id, comp_dict, normalize=True)

        metrics = [(k, v, comp_dict[k]) for k, v in metric_dict.iteritems()]
        numeric_metrics = [(k, v, comp_numeric_dict[k])
                           for k, v in numeric_dict.iteritems()]
    else:
        metrics = list(metric_dict.iteritems())
        numeric_metrics = list(numeric_dict.iteritems())

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

    dbms_id = db_conf.dbms.pk
    params_dict = JSONUtil.loads(db_conf.configuration)
    tuning_dict = DBMSUtil.filter_tunable_params(dbms_id, params_dict)

    if 'compare' in request.GET and request.GET['compare'] != 'none':
        compare_obj = DBConf.objects.get(pk=request.GET['compare'])
        comp_dict = JSONUtil.loads(compare_obj.configuration)
        comp_tuning_dict = DBMSUtil.filter_tunable_params(dbms_id, comp_dict)

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

    metric_meta = Statistics.objects.METRIC_META
    context = {'benchmark': benchmark_conf,
               'dbs': dbs,
               'metrics': metric_meta.keys(),
               'metric_meta': metric_meta,
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
                     y: int(y.summary_stats.throughput - x.summary_stats.throughput))

    data_package = {'results': [],
                    'error': 'None',
                    'metrics': data.get('met', 'throughput,p99_latency').split(',')}

    metric_meta = Statistics.objects.METRIC_META
    for met in data_package['metrics']:
        data_package['results'].append({'data': [[]], 'tick': [],
                                        'unit': metric_meta[met]['unit'],
                                        'lessisbetter': metric_meta[met]['improvement'],
                                        'metric': metric_meta[met]['print']})

        added = {}
        db_confs = data['db'].split(',')
        i = len(db_confs)
        for r in results:
            if r.dbms_config.pk in added or str(r.dbms_config.pk) not in db_confs:
                continue
            added[r.dbms_config.pk] = True
            data_package['results'][-1]['data'][0].append([
                i, getattr(r.summary_stats, met) * metric_meta[met]['scale'],
                r.pk, getattr(r.summary_stats, met) * metric_meta[met]['scale']])
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
    dbms_id = a.dbms.pk
    db_conf_a = DBMSUtil.filter_tunable_params(
        dbms_id, JSONUtil.loads(a.dbms_config.configuration))
    db_conf_b = DBMSUtil.filter_tunable_params(
        dbms_id, JSONUtil.loads(b.dbms_config.configuration))
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

    metric_meta = Statistics.objects.METRIC_META
    for metric, metric_info in metric_meta.iteritems():
        data_package[metric] = {
            'data': {},
            'units': metric_info['unit'],
            'lessisbetter': metric_info['improvement'],
            'metric': metric_info['print'],
            'print': metric_info['print'],
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

            ts = Statistics.objects.filter(data_result=x, type=StatsType.SAMPLES)
            if len(ts) > 0:
                offset = ts[0].time
                if len(ts) > 1:
                    offset -= ts[1].time - ts[0].time
                data_package[metric]['data'][int(x)] = []
                for t in ts:
                    data_package[metric]['data'][int(x)].append(
                        [t.time - offset, getattr(t, metric) * metric_meta[metric]['scale']])
                cache.set(key, data_package[metric]['data'][int(x)], 60 * 5)

    default_metrics = {}
    for met in Statistics.objects.DEFAULT_METRICS:
        default_metrics[met] = getattr(target.summary_stats, met) * metric_meta[met]['scale']

    status = None
    if target.task_ids is not None:
        task_ids = target.task_ids.split(',')
        tasks = []
        for tid in task_ids:
            task = TaskMeta.objects.filter(task_id=tid).first()
            if task is not None:
                tasks.append(task)
        status, _ = get_task_status(tasks)

    next_conf_available = True if status == 'SUCCESS' else False

    context = {
        'result': target,
        'metrics': metric_meta.keys(),
        'metric_meta': metric_meta,
        'default_metrics': default_metrics,
        'data': JSONUtil.dumps(data_package),
        'same_runs': same_dbconf_results,
        'status': status,
        'next_conf_available': next_conf_available,
        'similar_runs': similar_dbconf_results
    }
    return render(request, 'result.html', context)


@login_required(login_url='/login/')
def get_result_data_file(request):
    target = get_object_or_404(Result, pk=request.GET['id'])
    if target.application.user != request.user:
        return render(request, '404.html')

    result_id = int(request.GET['id'])
    result_type = request.GET['type']

    prefix = MediaUtil.get_result_data_path(result_id)

    if result_type == 'samples':
        filepath = prefix + '.samples'
    elif result_type == 'raw':
        filepath = prefix + '.raw'
    elif result_type == 'next_conf':
        filepath = prefix + '.next_conf'
    return MediaUtil.download_file(filepath)


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
    display_type = request.GET['ben']
    if display_type == 'show_none':
        pass
    else:

        if display_type == 'grid':
            benchmarks = set()
            benchmark_confs = []
            for result in results:
                benchmarks.add(result.benchmark_config.benchmark_type)
                benchmark_confs.append(result.benchmark_config)
        else:
            benchmarks = [display_type]
            benchmark_confs = filter(lambda x: x != '', request.GET[
                                     'spe'].strip().split(','))
            results = filter(lambda x: str(x.benchmark_config.pk)
                             in benchmark_confs, results)

#         if len(results) >= 1:

        for f in filter(lambda x: x != '', request.GET.getlist('add[]', [])):
            key, value = f.split(':')
            if value == 'select_all':
                continue
            results = filter(lambda x: getattr(
                x.benchmark_config, key) == value, results)

        table_results = results
        default_metrics = Statistics.objects.DEFAULT_METRICS
        if len(benchmarks) == 1:
            metrics = request.GET.get(
                'met', ','.join(default_metrics)).split(',')
        else:
            metrics = default_metrics

        # For the data table
        metric_meta = Statistics.objects.METRIC_META
        tput_scale = metric_meta[Statistics.objects.THROUGHPUT]['scale']
        lat_scale = metric_meta[Statistics.objects.P99_LATENCY]['scale']
        data_package['results'] = [
            [x.pk,
             x.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
             x.dbms_config.name,
             x.dbms_metrics.name,
             x.benchmark_config.name,
             x.summary_stats.throughput * tput_scale,
             x.summary_stats.p99_latency * lat_scale,
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

                data = {
                    'benchmark': bench,
                    'units': metric_meta[metric]['unit'],
                    'lessisbetter': metric_meta[metric]['improvement'],
                    'data': {},
                    'baseline': "None",
                    'metric': metric_meta[metric]['print']
                }

                for db in request.GET['db'].split(','):
                    d_r = filter(lambda x: x.dbms.key == db, b_r)
                    d_r = d_r[-revs:]
                    out = [
                        [
                            res.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                            getattr(res.summary_stats, metric) * metric_meta[metric]['scale'],
                            "",
                            str(res.pk)
                        ]
                        for res in d_r]

                    if len(out) > 0:
                        data['data'][db] = out

                data_package['timelines'].append(data)

    return HttpResponse(JSONUtil.dumps(data_package), content_type='application/json')
