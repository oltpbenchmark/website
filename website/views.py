from django.core.context_processors import csrf
from django.shortcuts import render_to_response, redirect, render
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.http import HttpResponse
import json
import random
import string
from django.template import RequestContext
from django.views.decorators.csrf import csrf_exempt
from models import Result, Project, DBConf, ExperimentConf, Statistics, NewResultForm, PLOTTABLE_FIELDS, METRIC_META
from django.utils.timezone import now
from website import settings


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


def upload_code_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))


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


@login_required(login_url='/login/')
def get_new_upload_code(request):
    proj = Project.objects.get(pk=request.POST['id'])
    proj.upload_code = upload_code_generator(size=20)
    proj.fallback_target_name = request.POST['fallback_target_name']
    proj.fallback_bench_name = request.POST['fallback_bench_name']
    proj.save()
    return redirect("/project/?id=" + str(proj.pk))


@login_required(login_url='/login/')
def home(request):
    context = {"projects": Project.objects.filter(user=request.user)}
    context.update(csrf(request))
    return render(request, 'home.html', context)


@login_required(login_url='/login/')
def project(request):
    id = request.GET['id']
    ps = Project.objects.filter(pk=id)
    if len(ps) != 1:
        redirect('/')
    p = ps[0]

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
            context['project'] = Project.objects.get(pk=request.GET['id'])
    except Project.DoesNotExist:
        pass
    return render(request, 'edit_project.html', context)


@login_required(login_url='/login/')
def delete_project(request):
    print request.POST.get('projects', [])
    map(lambda x: Project.objects.get(pk=x).delete(), request.POST.getlist('projects', []))
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
            return HttpResponse("Wrong")
        try:
            project = Project.objects.get(upload_code=form.cleaned_data['upload_code'])
        except Project.DoesNotExist:
            return HttpResponse("Wrong")

        return handle_result_file(project, request.FILES['data'])
    return HttpResponse("POST please\n")


def handle_result_file(proj, file_data):
    data = "".join(map(lambda x: str(x), file_data.chunks()))
    lines = data.split("\n")

    db_conf_cnt = int(lines[0])
    bench_conf_cnt = int(lines[1])
    sample_cnt = int(lines[2])
    summary_cnt = int(lines[3])

    header_cnt = 4

    db_conf_lines = lines[header_cnt:header_cnt + db_conf_cnt]
    bench_conf_lines = lines[header_cnt + db_conf_cnt: header_cnt + db_conf_cnt + bench_conf_cnt]
    sample_lines = lines[
                   1 + header_cnt + db_conf_cnt + bench_conf_cnt: header_cnt + db_conf_cnt + bench_conf_cnt + sample_cnt]
    summary_lines = lines[header_cnt + db_conf_cnt + bench_conf_cnt + sample_cnt:
    header_cnt + db_conf_cnt + bench_conf_cnt + sample_cnt + summary_cnt]

    db_type = summary_lines[0].strip().upper()
    bench_type = summary_lines[1].strip().upper()

    if not db_type in DBConf.DB_TYPES:
        return HttpResponse("Wrong")
    if not bench_type in ExperimentConf.BENCHMARK_TYPES:
        return HttpResponse("Wrong")

    db_conf_list = []
    for line in db_conf_lines:
        ele = line.split(":")
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
        db_conf.name = db_type + '@' + db_conf.creation_time.strftime("%Y-%m-%d:%H") + '#' + str(db_conf.pk)
        db_conf.save()

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
        id = 4
        for conf in ExperimentConf.FILTER_FIELDS:
            setattr(bench_conf, conf['field'], summary_lines[id])
            id += 1
        bench_conf.save()
        bench_conf.name = bench_type + '@' + bench_conf.creation_time.strftime("%Y-%m-%d:%H") + '#' + str(bench_conf.pk)
        bench_conf.save()

    result = Result()
    result.db_conf = db_conf
    result.benchmark_conf = bench_conf
    result.project = proj
    result.timestamp = now()
    latency_dict = {}
    line = summary_lines[2][1:-1]
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
    result.throughput = float(summary_lines[3])
    result.save()

    for line in sample_lines:
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


@login_required(login_url='/login/')
def db_conf_view(request):
    conf_str = DBConf.objects.get(pk=request.GET['id']).configuration
    conf = json.loads(conf_str, encoding="UTF-8")
    context = {'parameters': conf}
    return render(request, 'db_conf.html', context)


@login_required(login_url='/login/')
def benchmark_configuration(request):
    benchmark_conf = ExperimentConf.objects.get(pk=request.GET['id'])

    avai_db_confs = []
    dbs = {}
    for db_type in DBConf.DB_TYPES:
        dbs[db_type] = {}

        db_confs = DBConf.objects.filter(project=benchmark_conf.project, db_type=db_type)
        for db_conf in db_confs:
            rs = Result.objects.filter(db_conf=db_conf, benchmark_conf=benchmark_conf)
            if len(rs) < 1:
                continue
            r = rs.latest('timestamp')
            avai_db_confs.append(db_conf.pk)
            dbs[db_type][db_conf.name] = [db_conf, r]

        if len(dbs[db_type]) < 1:
            dbs.pop(db_type)

    context = {'benchmark': benchmark_conf,
               'dbs': dbs,
               'metrics': PLOTTABLE_FIELDS,
               'default_dbconf': avai_db_confs,
               'default_metrics': ['throughput', 'p99_latency']}
    return render(request, 'benchmark_conf.html', context)


@login_required(login_url='/login/')
def get_benchmark_data(request):
    data = request.GET

    benchmark_conf = ExperimentConf.objects.get(pk=data['id'])

    results = Result.objects.filter(benchmark_conf=benchmark_conf)

    bar_data = {'results': [], 'error': 'None', 'metrics': data.get('met', 'throughput,p99_latency').split(',')}

    index = {}
    for met in data.get('met', 'throughput,p99_latency').split(','):
        bar_data['results'].append({'data': [], 'tick': [],
                                    'unit': METRIC_META[met]['unit'],
                                    'lessisbetter': METRIC_META[met][
                                                        'lessisbetter'] and '(less is better)' or '(more is better)',
                                    'metric': met})
        index[met] = {'data': bar_data['results'][-1]['data'],
                      'tick': bar_data['results'][-1]['tick']}

    for db_conf in data.get('db', '').split(','):
        rs = filter(lambda x: str(x.db_conf.pk) == db_conf, results)
        if len(rs) == 0:
            continue
        r = rs[-1]
        for met in data.get('met', 'throughput,p99_latency').split(','):
            index[met]['data'].append(getattr(r, met) * METRIC_META[met]['scale'])
            index[met]['tick'].append(r.db_conf.pk)

    return HttpResponse(json.dumps(bar_data), mimetype='application/json')


@login_required(login_url='/login/')
def edit_benchmark_conf(request):
    context = {}
    try:
        if request.GET['id'] != '':
            context['benchmark'] = ExperimentConf.objects.get(pk=request.GET['id'])
    except ExperimentConf.DoesNotExist:
        return HttpResponse("Wrong")
    return render(request, 'edit_benchmark.html', context)


@login_required(login_url='/login/')
def result(request):
    ts = Statistics.objects.filter(result=request.GET['id'])
    offset = ts[0].time - (ts[1].time - ts[0].time)

    timelines = {}
    for field in PLOTTABLE_FIELDS:
        metric = field['field']
        timelines[metric] = {'data': [],
                             'units': METRIC_META[metric]['unit'],
                             'lessisbetter': METRIC_META[metric]['lessisbetter'] and '(less is better)' or '(more is better)',
                             'metric': field['print']
        }

        for t in ts:
            timelines[metric]['data'].append([t.time - offset, getattr(t, metric)])

    print timelines
    context = {'result': Result.objects.get(id=request.GET['id']),
               'metrics': PLOTTABLE_FIELDS,
               'default_metrics': ['throughput', 'p99_latency'],
               'data': json.dumps(timelines)}
    return render(request, 'result.html', context)


@login_required(login_url='/login/')
def get_result_data(request):
    field = request.GET['field']

    final_results = [{'key': field, 'values': result, 'color': '#ff7f0e'}]
    res = json.dumps(final_results, encoding="UTF-8")
    return HttpResponse(res, mimetype='application/json')


@login_required(login_url='/login/')
def get_data(request):
    revs = int(request.GET['revs'])
    timeline_list = {'error': 'None', 'timelines': []}
    results = Result.objects.filter(project=request.GET['proj'])

    dbs = request.GET['db'].split(',')
    results = filter(lambda x: x.db_conf.db_type in dbs, results)

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

    timeline_list['results'] = [[x.pk,
                                 x.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                                 x.db_conf.name,
                                 x.benchmark_conf.name,
                                 x.throughput * METRIC_META['throughput']['scale'],
                                 x.p99_latency * METRIC_META['p99_latency']['scale'],
                                 x.db_conf.pk,
                                 x.benchmark_conf.pk]
                                for x in table_results]

    for metric in metrics:
        for bench in benchmarks:
            b_r = filter(lambda x: x.benchmark_conf.benchmark_type == bench, results)
            if len(b_r) == 0:
                continue

            timeline = {
                'benchmark': bench,
                'units': METRIC_META[metric]['unit'],
                'lessisbetter': METRIC_META[metric]['lessisbetter'] and '(less is better)' or '(more is better)',
                'branches': {},
                'baseline': "None",
                'metric': metric
            }

            timeline['branches']['branch'] = {}

            for db in dbs:
                out = []
                d_r = filter(lambda x: x.db_conf.db_type == db, b_r)
                d_r = d_r[-revs:]
                for res in d_r:
                    out.append(
                        [
                            res.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                            getattr(res, metric) * METRIC_META[metric]['scale'], "",
                            str(res.pk)
                        ]
                    )

                if len(out) > 0:
                    timeline['branches']['branch'][db] = out

            timeline_list['timelines'].append(timeline)

    return HttpResponse(json.dumps(timeline_list), mimetype='application/json')
