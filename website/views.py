import json
import string

from django.core.context_processors import csrf
from django.shortcuts import redirect, render, get_object_or_404
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.http import HttpResponse
from django.template.defaultfilters import register
from django.utils.datetime_safe import datetime
from django.views.decorators.csrf import csrf_exempt
from django.utils.timezone import now
from math import log
from numpy import array, linalg
from pytz import timezone, os
from random import choice
from rexec import FileWrapper

from models import Result, Project, DBConf, ExperimentConf, Statistics, NewResultForm, PLOTTABLE_FIELDS, METRIC_META, FEATURED_VARS, LEARNING_VARS
from website.settings import UPLOAD_DIR

# For the html template to access dict object
@register.filter
def get_item(dictionary, key):
    return dictionary.get(key)


def signup_view(request):
    c = {}
    c.update(csrf(request))
    return render(request, 'signup.html', c)


def login_view(request):
    c = {}
    c.update(csrf(request))
    return render(request, 'login.html', c)


def auth_and_login(request, onsuccess='/', onfail='/login/'):
    user = authenticate(username=request.POST['email'], password=request.POST['password'])
    if user is not None:
        login(request, user)
        return redirect(onsuccess)
    else:
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
        create_user(username=post['email'], email=post['email'], password=post['password'])
        return auth_and_login(request)
    else:
        return redirect("/login/")


@login_required(login_url='/login/')
def logout_view(request):
    logout(request)
    return redirect("/login/")


def upload_code_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(choice(chars) for x in range(size))


@login_required(login_url='/login/')
def home(request):
    context = {"projects": Project.objects.filter(user=request.user)}
    context.update(csrf(request))
    return render(request, 'home.html', context)


@login_required(login_url='/login/')
def project(request):
    id = request.GET['id']
    p = Project.objects.get(pk=id)
    if p.user != request.user:
        return render(request, '404.html')

    data = request.GET

    project = Project.objects.get(pk=data['id'])

    results = Result.objects.filter(project=project)

    db_with_data = {}
    benchmark_with_data = {}

    for res in results:
        db_with_data[res.db_conf.db_type] = True
        benchmark_with_data[res.benchmark_conf.benchmark_type] = True
    benchmark_confs = set([res.benchmark_conf for res in results])

    dbs = [db for db in DBConf.DB_TYPES if db in db_with_data]
    benchmark_types = [benchmark for benchmark in ExperimentConf.BENCHMARK_TYPES if benchmark in benchmark_with_data]
    benchmarks = {}
    for benchmark in benchmark_types:
        specific_benchmark = [b for b in benchmark_confs if b.benchmark_type == benchmark]
        benchmarks[benchmark] = specific_benchmark

    lastrevisions = [10, 50, 200, 1000]

    filters = []
    for field in ExperimentConf.FILTER_FIELDS:
        value_dict = {}
        for res in results:
            value_dict[getattr(res.benchmark_conf, field['field'])] = True
        f = {'values': [key for key in value_dict.iterkeys()], 'print': field['print'], 'field': field['field']}
        filters.append(f)

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
               'project': p,
               'results': Result.objects.filter(project=p)}

    context.update(csrf(request))
    return render(request, 'project.html', context)


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
def delete_project(request):
    for pk in request.POST.getlist('projects', []):
        project = Project.objects.get(pk=pk)
        if project.user == request.user:
            project.delete()
    return redirect('/')


@login_required(login_url='/login/')
def update_project(request):
    if 'id_new_code' in request.POST:
        proj_id = request.POST['id_new_code']
    else:
        proj_id = request.POST['id']

    if proj_id == '':
        p = Project()
        p.creation_time = now()
        p.user = request.user
        p.upload_code = upload_code_generator(size=20)
    else:
        p = Project.objects.get(pk=proj_id)
        if p.user != request.user:
            return render(request, '404.html')

    if 'id_new_code' in request.POST:
        p.upload_code = upload_code_generator(size=20)

    p.name = request.POST['name']
    p.description = request.POST['description']
    p.last_update = now()
    p.save()
    return redirect('/project/?id=' + str(p.pk))


@csrf_exempt
def new_result(request):
    if request.method == 'POST':
        form = NewResultForm(request.POST, request.FILES)
        if not form.is_valid():
            return HttpResponse(str(form))
        try:
            project = Project.objects.get(upload_code=form.cleaned_data['upload_code'])
        except Project.DoesNotExist:
            return HttpResponse("Wrong!")

        return handle_result_file(project, request.FILES)

    return HttpResponse("POST please\n")


def get_result_data_dir(result_id):
    try:
        os.makedirs(UPLOAD_DIR + '/' + str(result_id % 100))
    except OSError:
        pass
    return UPLOAD_DIR + '/' + str(result_id % 100) + '/' + str(int(result_id) / 100l)


def handle_result_file(proj, files):
    db_conf_lines = "".join(map(lambda x: str(x), files['db_conf_data'].chunks())).split("\n")
    summary_lines = "".join(map(lambda x: str(x), files['summary_data'].chunks())).split("\n")

    db_type = summary_lines[1].strip().upper()
    bench_type = summary_lines[2].strip().upper()

    if not db_type in DBConf.DB_TYPES:
        return HttpResponse("Wrong")
    if not bench_type in ExperimentConf.BENCHMARK_TYPES:
        return HttpResponse("Wrong")

    db_conf_list = []
    for line in db_conf_lines:
        ele = line.split("=")
        key = ele[0]
        value = ""
        if len(ele) > 1:
            value = ele[1]
        db_conf_list.append([key, value])
    db_conf_str = json.dumps(db_conf_list)

    try:
        db_confs = DBConf.objects.filter(configuration=db_conf_str)
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
        db_conf.save()
        db_conf.name = db_type + '@' + db_conf.creation_time.strftime("%Y-%m-%d,%H") + '#' + str(db_conf.pk)
        db_conf.save()

    bench_conf_lines = "".join(map(lambda x: str(x).strip(), files['benchmark_conf_data'].chunks())).split("\n")
    bench_conf_str = "".join(bench_conf_lines)

    try:
        bench_confs = ExperimentConf.objects.filter(configuration=bench_conf_str)
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
        for line in summary_lines[5:]:
            if line == '':
                continue
            kv = line.split('=')
            setattr(bench_conf, kv[0], kv[1])
        bench_conf.save()
        bench_conf.name = bench_type + '@' + bench_conf.creation_time.strftime("%Y-%m-%d,%H") + '#' + str(bench_conf.pk)
        bench_conf.save()

    result = Result()
    result.db_conf = db_conf
    result.benchmark_conf = bench_conf
    result.project = proj
    result.timestamp = datetime.fromtimestamp(int(summary_lines[0]), timezone("UTC"))
    latency_dict = {}
    line = summary_lines[3][1:-1]
    for field in line.split(','):
        data = field.split('=')
        latency_dict[data[0].strip()] = data[1].strip()
    result.avg_latency = float(latency_dict['avg'])
    result.min_latency = float(latency_dict['min'])
    result.p25_latency = float(latency_dict['25th'])
    result.p50_latency = float(latency_dict['median'])
    result.p75_latency = float(latency_dict['75th'])
    result.p90_latency = float(latency_dict['90th'])
    result.p95_latency = float(latency_dict['95th'])
    result.p99_latency = float(latency_dict['99th'])
    result.max_latency = float(latency_dict['max'])
    result.throughput = float(summary_lines[4])
    result.save()

    path_prefix = get_result_data_dir(result.pk)
    with open(path_prefix + '_sample', 'wb') as dest:
        for chunk in files['sample_data'].chunks():
            dest.write(chunk)
        dest.close()
    with open(path_prefix + '_raw', 'wb') as dest:
        for chunk in files['raw_data'].chunks():
            dest.write(chunk)
        dest.close()

    sample_lines = "".join(map(lambda x: str(x), files['sample_data'].chunks())).split("\n")[1:]

    for line in sample_lines:
        if line.strip() == '':
            continue
        sta = Statistics()
        nums = line.split(",")
        sta.result = result
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
    for f in key_filters:
        if f.match(kv_pair[0]):
            return True
    return False


@login_required(login_url='/login/')
def db_conf_view(request):
    db_conf = DBConf.objects.get(pk=request.GET['id'])
    if db_conf.project.user != request.user:
        return render(request, '404.html')
    conf_str = db_conf.configuration
    conf = json.loads(conf_str, encoding="UTF-8")
    featured = filter(lambda x: filter_db_var(x, FEATURED_VARS[db_conf.db_type]), conf)

    if 'compare' in request.GET and request.GET['compare'] != 'none':
        compare_conf = DBConf.objects.get(pk=request.GET['compare'])
        compare_conf_list = json.loads(compare_conf.configuration, encoding='UTF-8')
        for a, b in zip(conf, compare_conf_list):
            a.extend(b[1:])
        for a, b in zip(featured, filter(lambda x: filter_db_var(x, FEATURED_VARS[db_conf.db_type]),
                                         json.loads(compare_conf.configuration, encoding='UTF-8'))):
            a.extend(b[1:])

    peer = DBConf.objects.filter(db_type=db_conf.db_type, project=db_conf.project)
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

        db_confs = DBConf.objects.filter(project=benchmark_conf.project, db_type=db_type)
        for db_conf in db_confs:
            rs = Result.objects.filter(db_conf=db_conf, benchmark_conf=benchmark_conf)
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


#Data Format
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
    benchmark_conf = get_object_or_404(ExperimentConf, pk=request.GET['id'])
    if benchmark_conf.project.user != request.user:
        return render(request, '404.html')

    return HttpResponse(benchmark_conf.configuration, content_type='text/plain')


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
    db_conf_a = json.loads(a.db_conf.configuration)
    db_conf_b = json.loads(b.db_conf.configuration)
    for kv in db_conf_a:
        if filter_db_var(kv, LEARNING_VARS[a.db_conf.db_type]):
            for bkv in db_conf_b:
                if bkv[0] == kv[0] and bkv[1] != kv[1]:
                    return False
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
                    except Exception:
                        values.append(0.0)
                        break

        features.append(values)

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
    for i in range(0, len(model)):
        score += abs(model[i] * float(values[i]))
    return score


@login_required(login_url='/login/')
def update_similar(request):
    target = get_object_or_404(Result, pk=request.GET['id'])
    results = Result.objects.filter(project=target.project, benchmark_conf=target.benchmark_conf)
    results = filter(lambda x: x.db_conf.db_type == target.db_conf.db_type, results)

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


#Data Format
#    data for each selected metric
#        meta data for the metric
#        data: time series data for this metric
@login_required(login_url='/login/')
def result(request):
    target = get_object_or_404(Result, pk=request.GET['id'])

    data_package = {}
    results = Result.objects.filter(project=target.project, benchmark_conf=target.benchmark_conf)
    results = filter(lambda x: x.db_conf.db_type == target.db_conf.db_type, results)
    sames = filter(lambda x: result_similar(x, target), results)
    sames = filter(lambda x: x != target, sames)
    similars = [Result.objects.get(pk=rid) for rid in filter(lambda x: len(x) > 0, target.most_similar.split(','))]

    results = []
    results.extend(sames)
    results.extend(similars)
    results.append(target)

    for metric in PLOTTABLE_FIELDS:
        data_package[metric] = {
            'data': {},
            'units': METRIC_META[metric]['unit'],
            'lessisbetter': METRIC_META[metric]['lessisbetter'] and '(less is better)' or '(more is better)',
            'metric': METRIC_META[metric]['print']
        }

        for r in results:
            ts = Statistics.objects.filter(result=r.pk)
            offset = ts[0].time
            if len(ts) > 1:
                offset -= ts[1].time - ts[0].time
            data_package[metric]['data'][r.pk] = []
            for t in ts:
                data_package[metric]['data'][r.pk].append(
                    [t.time - offset, getattr(t, metric) * METRIC_META[metric]['scale']])

    context = {'result': Result.objects.get(id=request.GET['id']),
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

    id = int(request.GET['id'])
    type = request.GET['type']

    prefix = get_result_data_dir(id)

    if type == 'sample':
        return HttpResponse(FileWrapper(file(prefix + '_' + type)), content_type='text/plain')
    elif type == 'raw':
        response = HttpResponse(FileWrapper(file(prefix + '_' + type)), content_type='application/gzip')
        response['Content-Disposition'] = 'attachment; filename=result_' + str(id) + '.raw.gz'
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

    project = get_object_or_404(Project, pk=request.GET['proj'])
    if project.user != request.user:
        return HttpResponse(json.dumps(data_package), content_type='application/json')

    revs = int(request.GET['revs'])

    # Get all results related to the selected DBMS, sort by time
    results = Result.objects.filter(project=request.GET['proj'])
    results = filter(lambda x: x.db_conf.db_type in request.GET['db'].split(','), results)
    results = sorted(results, cmp=lambda x, y: int((x.timestamp - y.timestamp).total_seconds()))

    # Determine which benchmark is selected
    benchmarks = []
    if request.GET['ben'] == 'grid':
        benchmarks = ExperimentConf.BENCHMARK_TYPES
        revs = 10
        results = filter(lambda x: x.benchmark_conf.benchmark_type in benchmarks, results)
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
                d_r = filter(lambda x: x.db_conf.db_type == db, b_r)
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
