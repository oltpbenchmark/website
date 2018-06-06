import functools
import json
import string

from random import choice

from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.core import serializers
from django.core.cache import cache
from django.http import HttpResponse
from django.shortcuts import redirect, render, get_object_or_404
from django.template.context_processors import csrf
from django.template.defaultfilters import register
from django.utils.datetime_safe import datetime
from django.utils.timezone import now
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_exempt
from pytz import timezone

from .models import (
    DBConf, ExperimentConf, NewResultForm, Project, Result,
    PLOTTABLE_FIELDS, METRIC_META
    )


# For the html template to access dict object
@register.filter
def get_item(dictionary, key):
    return dictionary.get(key)


def signup_view(request):
    csrf_dict = {}
    csrf_dict.update(csrf(request))
    return render(request, 'signup.html', csrf_dict)


def login_view(request):
    csrf_dict = {}
    csrf_dict.update(csrf(request))
    return render(request, 'login.html', csrf_dict)


def auth_and_login(request, onsuccess='/', onfail='/login/'):
    user = authenticate(
        username=request.POST['email'],
        password=request.POST['password']
    )

    if user is not None:
        login(request, user)
        return redirect(onsuccess)

    return redirect(onfail)


def create_user(username, email, password):
    user = User(username=username, email=email)
    user.set_password(password)
    user.save()
    return user


def user_exists(username):
    user_count = User.objects.filter(username=username).count()
    return user_count != 0


def sign_up_in(request):
    post = request.POST

    if not user_exists(post['email']):
        create_user(
            username=post['email'],
            email=post['email'],
            password=post['password']
        )
        return auth_and_login(request)

    return redirect("/login/")


@login_required(login_url='/login/')
def logout_view(request):
    logout(request)
    return redirect("/login/")


def upload_code_generator(size=6, chars=string.ascii_uppercase+string.digits):
    return ''.join(choice(chars) for x in range(size))


@login_required(login_url='/login/')
def home(request):
    context = {"projects": Project.objects.filter(user=request.user)}
    context.update(csrf(request))
    return render(request, 'home.html', context)


@login_required(login_url='/login/')
def project(request):
    data = request.GET
    proj = Project.objects.get(pk=data['id'])

    if proj.user != request.user:
        return render(request, '404.html')

    results = Result.objects.filter(project=proj)

    databases = set()
    benchmarks = {}
    for res in results:
        databases.add(res.db_conf.db_type)
        btype = res.benchmark_conf.benchmark_type
        benchmarks[btype] = benchmarks.get(btype, set())
        benchmarks[btype].add(res.benchmark_conf)

    filters = [{'values': list(set(getattr(res.benchmark_conf, field['field'])
                                   for res in results)),
                'print': field['print'],
                'field': field['field']}
               for field in ExperimentConf.FILTER_FIELDS
              ]

    context = {'project': proj,
               'db_types': databases,
               'benchmarks': benchmarks,
               'lastrevisions': [10, 50, 200, 1000],
               'defaultlast': 10,
               'defaultequid': False,
               'defaultbenchmark': 'grid',
               'metrics': PLOTTABLE_FIELDS,
               'metric_meta': METRIC_META,
               'defaultmetrics': ['throughput', 'p99_latency'],
               'filters': filters,
               'results': results}

    context.update(csrf(request))
    return render(request, 'project.html', context)


@login_required(login_url='/login/')
def edit_project(request):
    context = {}
    try:
        if request.GET['id'] != '':
            proj = Project.objects.get(pk=request.GET['id'])
            if proj.user != request.user:
                return render(request, '404.html')
            context['project'] = proj
    except Project.DoesNotExist:
        pass
    return render(request, 'edit_project.html', context)


@login_required(login_url='/login/')
def delete_project(request):
    for primary_key in request.POST.getlist('projects', []):
        proj = Project.objects.get(pk=primary_key)
        if proj.user == request.user:
            proj.delete()
    return redirect('/')


@login_required(login_url='/login/')
def update_project(request):
    if 'id_new_code' in request.POST:
        proj_id = request.POST['id_new_code']
    else:
        proj_id = request.POST['id']

    if proj_id == '':
        proj = Project()
        proj.creation_time = now()
        proj.user = request.user
        proj.upload_code = upload_code_generator(size=20)
    else:
        proj = Project.objects.get(pk=proj_id)
        if proj.user != request.user:
            return render(request, '404.html')

    if 'id_new_code' in request.POST:
        proj.upload_code = upload_code_generator(size=20)

    proj.name = request.POST['name']
    proj.description = request.POST['description']
    proj.last_update = now()
    proj.save()
    return redirect('/project/?id=' + str(proj.pk))


@csrf_exempt
def new_result(request):
    if request.method == 'POST':
        form = NewResultForm(request.POST, request.FILES)
        if not form.is_valid():
            return HttpResponse(str(form))

        try:
            proj = Project.objects.get(
                upload_code=form.cleaned_data['upload_code']
            )
        except Project.DoesNotExist:
            return HttpResponse("wrong upload_code!")

        upload_hash = form.cleaned_data['upload_hash']
        result_ok = form.cleaned_data['result_ok'].lower() == 'true'

        return handle_result_file(proj, request.FILES, upload_hash, result_ok)

    return HttpResponse("POST please\n")


def handle_result_file(proj, files, upload_hash, result_ok):
    summary_lines = json.loads(files['summary_data'].read().decode('utf-8'))

    db_type = summary_lines['DBMS Type'].strip().upper()
    bench_type = summary_lines['Benchmark Type'].strip().upper()

    if db_type not in DBConf.DB_TYPES:
        return HttpResponse(db_type + "  db_type Wrong")

    if bench_type not in ExperimentConf.BENCHMARK_TYPES:
        return HttpResponse(bench_type + "  bench_type  Wrong")

    def get_save_db_conf(db_type):
        db_conf = None
        try:
            db_confs = DBConf.objects.filter(db_type=db_type)
            if len(db_confs) < 1:
                raise DBConf.DoesNotExist
            db_conf = db_confs[0]
        except DBConf.DoesNotExist:
            db_conf = DBConf()
            db_conf.db_type = db_type
            db_conf.save()
        return db_conf

    def get_save_bench_conf(proj, benchmark_conf_data, bench_type):
        b_chunks = [str(x).strip() for x in benchmark_conf_data.chunks()]
        bench_conf_str = ''.join(''.join(b_chunks).split('\n'))

        bench_conf = None
        try:
            bench_confs = ExperimentConf.objects.filter(
                configuration=bench_conf_str
            )
            if len(bench_confs) < 1:
                raise ExperimentConf.DoesNotExist
            bench_conf = bench_confs[0]
        except ExperimentConf.DoesNotExist:
            bench_conf = ExperimentConf()
            bench_conf.project = proj
            bench_conf.configuration = bench_conf_str
            bench_conf.benchmark_type = bench_type
            bench_conf.creation_time = now()
            for key, val in summary_lines.items():
                unwanted = [
                    'Benchmark Type',
                    'Current Timestamp (milliseconds)',
                    'DBMS Type',
                    'DBMS Version',
                    'Latency Distribution',
                    'Throughput (requests/second)',
                ]
                if key not in unwanted:
                    setattr(bench_conf, key, val)
            bench_conf.name = ''.join([
                bench_type,
                '@',
                bench_conf.creation_time.strftime("%Y-%m-%d,%H"),
                '#',
                str(bench_conf.pk)
            ])
            bench_conf.save()
        return bench_conf

    def save_result(proj, db_conf, bench_conf, summary_lines):
        res = Result()
        res.db_conf = db_conf
        res.benchmark_conf = bench_conf
        res.project = proj
        res.timestamp = datetime.fromtimestamp(
            summary_lines['Current Timestamp (milliseconds)'] // 1000,
            timezone("UTC")
        )

        latency_dict = summary_lines['Latency Distribution']

        res.avg_latency = \
            float(latency_dict['Average Latency (microseconds)'])
        res.min_latency = \
            float(latency_dict['Minimum Latency (microseconds)'])
        res.p25_latency = \
            float(latency_dict['25th Percentile Latency (microseconds)'])
        res.p50_latency = \
            float(latency_dict['Median Latency (microseconds)'])
        res.p75_latency = \
            float(latency_dict['75th Percentile Latency (microseconds)'])
        res.p90_latency = \
            float(latency_dict['90th Percentile Latency (microseconds)'])
        res.p95_latency = \
            float(latency_dict['95th Percentile Latency (microseconds)'])
        res.p99_latency = \
            float(latency_dict['99th Percentile Latency (microseconds)'])
        res.max_latency = \
            float(latency_dict['Maximum Latency (microseconds)'])
        res.throughput = \
            float(summary_lines['Throughput (requests/second)'])
        res.git_hash = upload_hash
        res.result_ok = result_ok
        res.save()

    db_conf = get_save_db_conf(db_type)
    bench_conf = get_save_bench_conf(proj,
                                     files['benchmark_conf_data'],
                                     bench_type)
    save_result(proj, db_conf, bench_conf, summary_lines)

    proj.last_update = now()
    proj.save()

    return HttpResponse('Success')


@login_required(login_url='/login/')
def benchmark_configuration(request):
    benchmark_conf = get_object_or_404(ExperimentConf, pk=request.GET['id'])

    if benchmark_conf.project.user != request.user:
        return render(request, '404.html')

    results = Result.objects.filter(benchmark_conf=benchmark_conf)
    dbs = list(set(res.db_conf.db_type for res in results))

    context = {'benchmark': benchmark_conf,
               'dbs': dbs,
               'metrics': PLOTTABLE_FIELDS,
               'metric_meta': METRIC_META,
               'default_dbconf': dbs,
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

    if benchmark_conf.project.user != request.user:
        return render(request, '404.html')

    def _throughput_diff(fst, snd):
        return int(snd.throughput - fst.throughput)

    results = Result.objects.filter(benchmark_conf=benchmark_conf)
    results = sorted(results, key=functools.cmp_to_key(_throughput_diff))

    data_package = {
        'results': [],
        'error': 'None',
        'metrics': data.get('met', 'throughput,p99_latency').split(',')
    }

    for met in data_package['metrics']:
        data_package['results']. \
            append({'data': [[]], 'tick': [],
                    'unit': METRIC_META[met]['unit'],
                    'lessisbetter': '(less is better)'
                                    if METRIC_META[met]['lessisbetter']
                                    else '(more is better)',
                    'metric': METRIC_META[met]['print']})

        added = {}
        db_confs = data['db'].split(',')
        i = len(db_confs)
        for res in results:
            if res.db_conf.pk in added or str(res.db_conf.pk) not in db_confs:
                continue
            added[res.db_conf.pk] = True
            data_package['results'][-1]['data'][0].append([
                i,
                res.met * METRIC_META[met]['scale'],
                res.pk,
                res.met * METRIC_META[met]['scale']
            ])
            data_package['results'][-1]['tick'].append(res.db_conf.name)
            i -= 1
        data_package['results'][-1]['data'].reverse()
        data_package['results'][-1]['tick'].reverse()

    return HttpResponse(
        json.dumps(data_package),
        content_type='application/json')


@login_required(login_url='/login/')
def get_benchmark_conf_file(request):
    benchmark_conf = get_object_or_404(ExperimentConf, pk=request.GET['id'])
    if benchmark_conf.project.user != request.user:
        return render(request, '404.html')

    return HttpResponse(
        benchmark_conf.configuration,
        content_type='text/plain')


@login_required(login_url='/login/')
def edit_benchmark_conf(request):
    context = {}
    if request.GET['id'] != '':
        ben_conf = get_object_or_404(ExperimentConf, pk=request.GET['id'])
        if ben_conf.project.user != request.user:
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


def result(request):
    target = get_object_or_404(Result, pk=request.GET['id'])
    data_package = {}

    results = Result.objects.filter(project=target.project,
                                    benchmark_conf=target.benchmark_conf)
    results = [r for r in results
               if r.db_conf.db_type == target.db_conf.db_type]

    sames = [r for r in results
             if r.benchmark_conf == target.benchmark_conf and r != target]

    for metric in PLOTTABLE_FIELDS:
        data_package[metric] = {
            'data': {},
            'units': METRIC_META[metric]['unit'],
            'lessisbetter': '(less is better)'
                            if METRIC_META[metric]['lessisbetter']
                            else '(more is better)',
            'metric': METRIC_META[metric]['print']
        }

        same_id = []
        same_id.append(str(target.pk))

    context = {
        'result': target,
        'metrics': PLOTTABLE_FIELDS,
        'metric_meta': METRIC_META,
        'default_metrics': ['throughput', 'p99_latency'],
        'data': json.dumps(data_package),
        'same_runs': sames
    }
    return render(request, 'result.html', context)


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

    proj = get_object_or_404(Project, pk=request.GET['proj'])

    if proj.user != request.user:
        return HttpResponse(
            json.dumps(data_package),
            content_type='application/json')

    revs = int(request.GET['revs'])

    # Get all results related to the selected DBMS, sort by time
    results = Result.objects.filter(project=request.GET['proj'])

    def _valid_db(x):
        return x.db_conf.db_type in request.GET['db'].split(',')

    def cmptime(x, y):
        return int((x.timestamp - y.timestamp).total_seconds())

    results = [r for r in results if _valid_db(r)]
    results = sorted(results, key=functools.cmp_to_key(cmptime))

    # Determine which benchmark is selected
    benchmarks = []
    if request.GET['ben'] == 'grid':
        benchmarks = ExperimentConf.BENCHMARK_TYPES
        revs = 10

        def _in_benchmarks(x):
            return x.benchmark_conf.benchmark_type in benchmarks

        results = [r for r in results if _in_benchmarks(r)]
        table_results = []
    elif request.GET['ben'] == 'show_none':
        benchmarks = []
        table_results = []
    else:
        benchmarks = [request.GET['ben']]
        benchmark_confs = [x for x in request.GET['spe'].strip().split(',')
                           if x != '']

        def _in_confs(x):
            return str(x.benchmark_conf.pk) in benchmark_confs

        results = [r for r in results if _in_confs(r)]

        for f in [r for r in request.GET.getlist('add[]', []) if r != '']:
            _, value = f.split(':')
            if value == 'select_all':
                continue
            results = [r for r in results if r.benchmark_conf.key == value]

        table_results = results

    if len(benchmarks) == 1:
        metrics = request.GET.get('met', 'throughput,p99_latency').split(',')
    else:
        metrics = ['throughput']

    # For the data table
    data_package['results'] = [
        [
            x.pk,
            x.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            x.db_conf.db_type,
            x.benchmark_conf.name,
            x.throughput * METRIC_META['throughput']['scale'],
            x.p99_latency * METRIC_META['p99_latency']['scale'],
            x.db_conf.pk,
            x.benchmark_conf.pk
        ]
        for x in table_results
    ]

    # For plotting charts
    for metric in metrics:
        for bench in benchmarks:
            b_r = [r for r in results
                   if r.benchmark_conf.benchmark_type == bench]

            if not b_r:
                continue

            data = {
                'benchmark': bench,
                'units': METRIC_META[metric]['unit'],
                'lessisbetter': '(less is better)'
                                if METRIC_META[metric]['lessisbetter']
                                else '(more is better)',
                'data': {},
                'baseline': "None",
                'metric': metric
            }

            for db in request.GET['db'].split(','):
                d_r = [b for b in b_r if b.db_conf.db_type == db]
                d_r = d_r[-revs:]
                out = [
                    [
                        res.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                        getattr(res, metric) * METRIC_META[metric]['scale'],
                        "",
                        str(res.pk),
                        res.git_hash
                    ]
                    for res in d_r]

                if out:
                    data['data'][db] = out

            data_package['timelines'].append(data)

    return HttpResponse(
        json.dumps(data_package),
        content_type='application/json'
        )

@never_cache
def get_recent_data(request):
    upload_code = request.GET['upload_code']
    benchmark_name = request.GET['bench_name']
    res = Result.objects.filter(project__upload_code=upload_code,
                                benchmark_conf__benchmark_type=benchmark_name)
    json_data = '[]'
    if res:
        latest = res.latest('timestamp')
        json_data = serializers.serialize('json', [latest])
    return HttpResponse(
        json_data,
        content_type='application/json'
        )
