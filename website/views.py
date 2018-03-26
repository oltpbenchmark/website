import functools
import json
import string


from math import log
from random import choice
from wsgiref.util import FileWrapper

from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.core.cache import cache
from django.http import HttpResponse
from django.shortcuts import redirect, render, get_object_or_404
from django.template.context_processors import csrf
from django.template.defaultfilters import register
from django.utils.datetime_safe import datetime
from django.utils.timezone import now
from django.views.decorators.csrf import csrf_exempt
from pytz import timezone, os
from website.settings import UPLOAD_DIR

from .models import (
    Result, Project, DBConf, ExperimentConf, Statistics, NewResultForm,
    PLOTTABLE_FIELDS, METRIC_META, FEATURED_VARS, LEARNING_VARS
    )


# For the html template to access dict object
@register.filter
def get_item(dictionary, key):
    return dictionary.get(key)


def ajax_new(request):
    new_id = request.GET['new_id']
    tstats = Statistics.objects.filter(result=new_id)
    data = {}
    for metric in PLOTTABLE_FIELDS:
        if tstats:
            offset = tstats[0].time
            if len(tstats) > 1:
                offset -= tstats[1].time - tstats[0].time
            data[metric] = []
            for stat in tstats:
                data[metric].append([
                    stat.time - offset,
                    stat.metric * METRIC_META[metric]['scale']
                ])
    return HttpResponse(json.dumps(data), content_type='application/json')


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
    if user_count == 0:
        return False
    return True


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

    db_with_data = {}
    benchmark_with_data = {}

    for res in results:
        db_with_data[res.db_conf.db_type] = True
        benchmark_with_data[res.benchmark_conf.benchmark_type] = True
    benchmark_confs = set([res.benchmark_conf for res in results])

    dbs = [db for db in DBConf.DB_TYPES if db in db_with_data]
    benchmark_types = [b for b in ExperimentConf.BENCHMARK_TYPES
                       if b in benchmark_with_data]
    benchmarks = {}
    for benchmark in benchmark_types:
        specific_benchmark = [b for b in benchmark_confs
                              if b.benchmark_type == benchmark]
        benchmarks[benchmark] = specific_benchmark

    lastrevisions = [10, 50, 200, 1000]

    filters = []
    for field in ExperimentConf.FILTER_FIELDS:
        value_dict = {}
        for res in results:
            value_dict[getattr(res.benchmark_conf, field['field'])] = True
        new_filter = {
            'values': [key for key in value_dict],
            'print': field['print'],
            'field': field['field']
        }
        filters.append(new_filter)

    context = {'project': proj,
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
               'results': Result.objects.filter(project=proj)}

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

        return handle_result_file(proj, request.FILES)

    return HttpResponse("POST please\n")


def get_result_data_dir(result_id):
    result_path = os.path.join(UPLOAD_DIR, str(result_id % 100))
    try:
        os.makedirs(result_path)
    except OSError as err:
        if err.errno == 17:
            pass
    return os.path.join(result_path, str(int(result_id) / 100))


def handle_result_file(proj, files):
    p_chunks = [str(x) for x in files['db_parameters_data'].chunks()]
    db_conf_lines = "".join(p_chunks).split("\n")
    summary_lines = json.loads(files['summary_data'].read().decode('utf-8'))

    db_type = summary_lines['DBMS Type'].strip().upper()
    bench_type = summary_lines['Benchmark Type'].strip().upper()

    if db_type not in DBConf.DB_TYPES:
        return HttpResponse(db_type + "  db_type Wrong")
    if bench_type not in ExperimentConf.BENCHMARK_TYPES:
        return HttpResponse(bench_type + "  bench_type  Wrong")

    db_conf_list = []
    similar_conf_list = []
    for line in db_conf_lines:
        ele = line.split("=")
        key = ele[0]
        value = ""
        if len(ele) > 1:
            value = ele[1]
        for var in LEARNING_VARS[db_type]:
            if var.match(key):
                similar_conf_list.append([key, value])
        db_conf_list.append([key, value])

    db_conf_str = json.dumps(db_conf_list)
    similar_conf_str = json.dumps(similar_conf_list)
    try:
        db_confs = DBConf.objects.filter(
            configuration=db_conf_str,
            similar_conf=similar_conf_str)
        if len(db_confs) < 1:
            raise DBConf.DoesNotExist
        db_conf = db_confs[0]
    except DBConf.DoesNotExist:
        db_conf = DBConf()
        db_conf.creation_time = now()
        db_conf.name = ''
        db_conf.configuration = db_conf_str
        db_conf.project = proj
        db_conf.db_type = db_type
        db_conf.similar_conf = similar_conf_str
        db_conf.save()
        db_conf.name = ''.join([
            db_type,
            '@',
            db_conf.creation_time.strftime("%Y-%m-%d,%H"),
            '#',
            str(db_conf.pk)
        ])
        db_conf.save()

    b_chunks = [str(x).strip() for x in files['benchmark_conf_data'].chunks()]
    bench_conf_lines = "".join(b_chunks).split("\n")
    bench_conf_str = "".join(bench_conf_lines)

    try:
        bench_confs = ExperimentConf.objects.filter(
            configuration=bench_conf_str
        )
        if len(bench_confs) < 1:
            raise ExperimentConf.DoesNotExist
        bench_conf = bench_confs[0]
    except ExperimentConf.DoesNotExist:
        bench_conf = ExperimentConf()
        bench_conf.name = ''
        bench_conf.project = proj
        bench_conf.configuration = bench_conf_str
        bench_conf.benchmark_type = bench_type
        bench_conf.creation_time = now()
        for k, v in summary_lines.items():
            unwanted = [
                'Benchmark Type',
                'Current Timestamp (milliseconds)',
                'DBMS Type',
                'DBMS Version',
                'Latency Distribution',
                'Throughput (requests/second)',
            ]
            if k not in unwanted:
                setattr(bench_conf, k, v)
        bench_conf.save()
        bench_conf.name = ''.join([
            bench_type,
            '@',
            bench_conf.creation_time.strftime("%Y-%m-%d,%H"),
            '#',
            str(bench_conf.pk)
        ])

        bench_conf.save()

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
    res.save()

    path_prefix = get_result_data_dir(res.pk)
    with open(path_prefix + '_sample', 'wb') as dest:
        for chunk in files['sample_data'].chunks():
            dest.write(chunk)
        dest.close()
    with open(path_prefix + '_raw', 'wb') as dest:
        for chunk in files['raw_data'].chunks():
            dest.write(chunk)
        dest.close()

    sample_chunks = [str(x) for x in files['sample_data'].chunks()]
    sample_lines = "".join(sample_chunks).split("\n")[1:]

    for line in sample_lines:
        if line.strip() == '':
            continue
        sta = Statistics()
        nums = line.split(",")
        sta.result = res
        sta.time = int(nums[0])
        sta.throughput = float(nums[1])
        sta.avg_latency = float(nums[2])
        sta.min_latency = float(nums[3])
        sta.p25_latency = float(nums[4])
        sta.p50_latency = float(nums[5])
        sta.p75_latency = float(nums[6])
        sta.p90_latency = float(nums[7])
        sta.p95_latency = float(nums[8])
        sta.p99_latency = float(nums[9])
        sta.max_latency = float(nums[10])
        sta.save()

    proj.last_update = now()
    proj.save()

    return HttpResponse("Success")


def filter_db_var(kv_pair, key_filters):
    for fil in key_filters:
        if fil.match(kv_pair[0]):
            return True
    return False


@login_required(login_url='/login/')
def db_conf_view(request):
    db_conf = DBConf.objects.get(pk=request.GET['id'])
    if db_conf.project.user != request.user:
        return render(request, '404.html')
    conf_str = db_conf.configuration
    conf = json.loads(conf_str, encoding="UTF-8")
    featured = [c for c in conf
                if filter_db_var(c, FEATURED_VARS[db_conf.db_type])]

    if 'compare' in request.GET and request.GET['compare'] != 'none':
        compare_conf = DBConf.objects.get(pk=request.GET['compare'])
        compare_conf_list = json.loads(compare_conf.configuration,
                                       encoding='UTF-8')

        for a, b in zip(conf, compare_conf_list):
            a.extend(b[1:])

        filtered = filter(
            lambda x: filter_db_var(x, FEATURED_VARS[db_conf.db_type]),
            json.loads(compare_conf.configuration, encoding='UTF-8')
        )

        for a, b in zip(featured, filtered):
            a.extend(b[1:])

    peer = DBConf.objects.filter(db_type=db_conf.db_type,
                                 project=db_conf.project)
    peer_db_conf = [[c.name, c.pk] for c in peer if c.pk != db_conf.pk]

    context = {'parameters': conf,
               'featured_par': featured,
               'db_conf': db_conf,
               'compare': request.GET.get('compare', 'none'),
               'peer_db_conf': peer_db_conf}
    return render(request, 'db_conf.html', context)


@login_required(login_url='/login/')
def benchmark_configuration(request):
    benchmark_conf = get_object_or_404(ExperimentConf, pk=request.GET['id'])

    if benchmark_conf.project.user != request.user:
        return render(request, '404.html')

    all_db_confs = []
    dbs = {}
    for db_type in DBConf.DB_TYPES:
        dbs[db_type] = {}

        db_confs = DBConf.objects.filter(project=benchmark_conf.project,
                                         db_type=db_type)
        for db_conf in db_confs:
            rs = Result.objects.filter(db_conf=db_conf,
                                       benchmark_conf=benchmark_conf)
            if len(rs) < 1:
                continue
            r = rs.latest('timestamp')
            all_db_confs.append(db_conf.pk)
            dbs[db_type][db_conf.name] = [db_conf, r]

        if len(dbs[db_type]) < 1:
            dbs.pop(db_type)

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

    if benchmark_conf.project.user != request.user:
        return render(request, '404.html')

    def _throughput_diff(x, y):
        return int(y.throughput - x.throughput)

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
        for r in results:
            if r.db_conf.pk in added or str(r.db_conf.pk) not in db_confs:
                continue
            added[r.db_conf.pk] = True
            data_package['results'][-1]['data'][0].append([
                i,
                r.met * METRIC_META[met]['scale'],
                r.pk,
                r.met * METRIC_META[met]['scale']
            ])
            data_package['results'][-1]['tick'].append(r.db_conf.name)
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


def result_similar(a, b):
    db_conf_a = json.loads(a.db_conf.similar_conf)
    db_conf_b = json.loads(b.db_conf.similar_conf)
    for kv in db_conf_a:
        for bkv in db_conf_b:
            if bkv[0] == kv[0]:
                if bkv[1] != kv[1]:
                    return False
        else:
            break
    return True


def learn_model(results):
    features = []
    for f in LEARNING_VARS[results[0].db_conf.db_type]:
        values = []
        for r in results:
            db_conf = json.loads(r.db_conf.configuration)
            for kv in db_conf:
                if f.match(kv[0]):
                    try:
                        values.append(log(int(kv[1])))
                        break
                    except ValueError:
                        values.append(0.0)
                        break

        features.append(values)

    # this will now throw an error since we removed the np dependency
    A = array(features)
    y = [r.throughput for r in results]
    w = linalg.lstsq(A.T, y)[0]

    return w


def apply_model(model, data, target):
    values = []
    db_conf = json.loads(data.db_conf.configuration)
    db_conf_t = json.loads(target.db_conf.configuration)
    for f in LEARNING_VARS[data.db_conf.db_type]:
        v1 = 0
        v2 = 0
        for kv in db_conf:
            if f.match(kv[0]):
                if kv[1] == '0':
                    kv[1] = '1'
                v1 = log(int(kv[1]))
        for kv in db_conf_t:
            if f.match(kv[0]):
                if kv[1] == '0':
                    kv[1] = '1'
                v2 = log(int(kv[1]))
        values.append(v1 - v2)

    score = 0
    for i, mod in enumerate(model):
        score += abs(mod * float(values[i]))
    return score


@login_required(login_url='/login/')
def update_similar(request):
    target = get_object_or_404(Result, pk=request.GET['id'])
    results = Result.objects.filter(project=target.project,
                                    benchmark_conf=target.benchmark_conf)
    results = [r for r in results
               if r.db_conf.db_type == target.db_conf.db_type]

    linear_model = learn_model(results)
    diff_results = filter(lambda x: x != target, results)
    diff_results = [r for r in diff_results if not result_similar(r, target)]
    scores = [apply_model(linear_model, x, target) for x in diff_results]

    def _score(x, y):
        if x[1] > y[1]:
            return 1
        elif x[1] < y[1]:
            return -1
        return 0

    similars = sorted(
        zip(diff_results, scores),
        key=functools.cmp_to_key(_score)
    )

    if len(similars) > 5:
        similars = similars[:5]

    target.most_similar = ','.join([str(r[0].pk) for r in similars])
    target.save()

    return redirect('/result/?id=' + str(request.GET['id']))


def result(request):
    target = get_object_or_404(Result, pk=request.GET['id'])
    data_package = {}
    sames = {}
    similars = {}

    results = Result.objects.filter(project=target.project,
                                    benchmark_conf=target.benchmark_conf)
    results = [r for r in results
               if r.db_conf.db_type == target.db_conf.db_type]

    sames = []
    sames = [r for r in results if result_similar(r, target) and r != target]
    rids = [x for x in target.most_similar.split(',') if len(x) > 0]
    similars = [Result.objects.get(pk=rid) for rid in rids]

    results = []

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
        for x in same_id:
            key = metric + ',data,' + x
            tmp = cache.get(key)
            if tmp is not None:
                data_package[metric]['data'][int(x)] = []
                data_package[metric]['data'][int(x)].extend(tmp)
                continue

            ts = Statistics.objects.filter(result=x)
            if ts:
                offset = ts[0].time
                if len(ts) > 1:
                    offset -= ts[1].time - ts[0].time
                data_package[metric]['data'][int(x)] = []
                for t in ts:
                    data_package[metric]['data'][int(x)].append([
                        t.time - offset,
                        t.metric * METRIC_META[metric]['scale']
                    ])
            cache.set(key, data_package[metric]['data'][int(x)], 60*5)

    context = {
        'result': target,
        'metrics': PLOTTABLE_FIELDS,
        'metric_meta': METRIC_META,
        'default_metrics': ['throughput', 'p99_latency'],
        'data': json.dumps(data_package),
        'same_runs': sames,
        'similar_runs': similars
    }
    return render(request, 'result.html', context)


@login_required(login_url='/login/')
def get_result_data_file(request):
    target = get_object_or_404(Result, pk=request.GET['id'])

    if target.project.user != request.user:
        return render(request, '404.html')

    _id = int(request.GET['id'])
    _type = request.GET['type']

    prefix = get_result_data_dir(_id)

    if _type == 'sample':
        return HttpResponse(
            FileWrapper(open(prefix + '_' + _type)),
            content_type='text/plain')
    elif _type == 'raw':
        response = HttpResponse(
            FileWrapper(open(prefix + '_' + _type)),
            content_type='application/gzip')
        response['Content-Disposition'] = \
            'attachment; filename=result_' + str(_id) + '.raw.gz'
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
            x.db_conf.name,
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
                        str(res.pk)
                    ]
                    for res in d_r]

                if out:
                    data['data'][db] = out

            data_package['timelines'].append(data)

    return HttpResponse(
        json.dumps(data_package),
        content_type='application/json'
        )
