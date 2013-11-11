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
from models import Result, Project, DBConf, ExperimentConf, Environment, Statistics, NewResultForm, PLOTTABLE_FIELDS
from django.utils.timezone import now
from website import settings


def no_environment_error(request):
    return render_to_response('codespeed/nodata.html', {
        'message': 'You need to configure at least one Environment.'
    }, context_instance=RequestContext(request))


def get_default_environment(enviros, data, multi=False):
    """Returns the default environment. Preference level is:
        * Present in URL parameters (permalinks)
        * Value in settings.py
        * First Environment ID

    """
    defaultenviros = []
    # Use permalink values
    if 'env' in data:
        for env_value in data['env'].split(","):
            for env in enviros:
                try:
                    env_id = int(env_value)
                except ValueError:
                    # Not an int
                    continue
                for env in enviros:
                    if env_id == env.id:
                        defaultenviros.append(env)
            if not multi:
                break
    # Use settings.py value
    if not defaultenviros and not multi:
        if (hasattr(settings, 'DEF_ENVIRONMENT') and
                settings.DEF_ENVIRONMENT is not None):
            for env in enviros:
                if settings.DEF_ENVIRONMENT == env.name:
                    defaultenviros.append(env)
                    break
    # Last fallback
    if not defaultenviros:
        defaultenviros = enviros
    if multi:
        return defaultenviros
    else:
        return defaultenviros[0]


def login_view(request):
    c = {}
    c.update(csrf(request))
    return render_to_response('login.html', c)


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
    context = {"projects": Project.objects.filter(user=request.user),
               "environments": Environment.objects.filter(user=request.user)}
    context.update(csrf(request))
    return render(request, 'home.html', context)


@login_required(login_url='/login/')
def edit_env(request):
    context = {}
    context.update(csrf(request))
    try:
        if request.GET['id'] != '':
            context['environment'] = Environment.objects.get(pk=request.GET['id'])
    except Environment.DoesNotExist:
        pass
    return render(request, 'edit_env.html', context)


@login_required(login_url='/login/')
def update_env(request):
    env_id = request.POST.get('id', '')
    if env_id != '':
        env = Environment.objects.get(pk=request.POST['id'])
    else:
        env = Environment()
        env.creation_time = now()
        env.user = request.user
    env.name = request.POST['name']
    env.description = request.POST['description']
    env.save()
    return redirect('/')


@login_required(login_url='/login/')
def delete_env(request):
    map(lambda x: Environment.objects.get(pk=x).delete(), request.POST.getlist('environments', []))
    return redirect('/')


@login_required(login_url='/login/')
def project(request):
    id = request.GET['id']
    ps = Project.objects.filter(pk=id)
    if len(ps) != 1:
        redirect('/')
    p = ps[0]
    context = {'project': p,
               'results': Result.objects.filter(project=p),
               'environments': Environment.objects.filter(user=request.user)}
    context.update(csrf(request))
    return render(request, 'project.html', context)


@login_required(login_url='/login/')
def edit_project(request):
    context = {'environments': Environment.objects.filter(user=request.user)}
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
    p.environment = Environment.objects.get(pk=request.POST['environment'])
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
    sample_lines = lines[1 + header_cnt + db_conf_cnt + bench_conf_cnt: header_cnt + db_conf_cnt + bench_conf_cnt + sample_cnt]
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
        db_conf.name = ''
        db_conf.configuration = db_conf_str
        db_conf.project = proj
        db_conf.db_type = db_type
        db_conf.save()
        db_conf.name = db_type + '#' + str(db_conf.pk)
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
        bench_conf.save()
        bench_conf.name = bench_type + '#' + str(bench_conf.pk)
        bench_conf.save()

    result = Result()
    result.db_conf = db_conf
    result.benchmark_conf = bench_conf
    result.environment = proj.environment
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
def target_configuration(request):
    conf_str = DBConf.objects.get(pk=request.GET['id']).configuration
    conf = json.loads(conf_str, encoding="UTF-8")
    context = {'parameters': conf}
    return render(request, 'db_conf.html', context)


@login_required(login_url='/login/')
def benchmark_configuration(request):
    conf_str = ExperimentConf.objects.get(pk=request.GET['id']).configuration
    return HttpResponse(conf_str, content_type="application/xml")


@login_required(login_url='/login/')
def result(request):
    fields = []
    for member in PLOTTABLE_FIELDS:
        fields.append(member)
    context = {'result': Result.objects.get(id=request.GET['id']),
               'fields': fields}
    return render(request, 'result.html', context)


@login_required(login_url='/login/')
def get_result_data(request):
    ts = Statistics.objects.filter(result=request.GET['id'])
    result = []
    offset = ts[0].time
    field = request.GET['field']
    for t in ts:
        result.append({'x': t.time - offset, 'y': getattr(t, field)})
    final_results = [{'key': field, 'values': result, 'color': '#ff7f0e'}]
    res = json.dumps(final_results, encoding="UTF-8")
    return HttpResponse(res, mimetype='application/json')


@login_required(login_url='/login/')
def timeline(request):
    data = request.GET

    project = Project.objects.get(pk=data['id'])

    enviros = Environment.objects.all()
    if not enviros:
        return no_environment_error(request)
    defaultenviro = get_default_environment(enviros, data)

    lastrevisions = [10, 50, 200, 1000]
    context = {'project': project,
               'db_types': DBConf.DB_TYPES,
               'benchmarks': ExperimentConf.BENCHMARK_TYPES,
               'lastrevisions': lastrevisions,
               'defaultlast': 10,
               'defaultequid': False,
               'environments': enviros,
               'defaultenvironment': defaultenviro,
               'defaultbenchmark': 'TPCC'
               }
    return render(request, 'timeline.html', context)


@login_required(login_url='/login/')
def get_data(request):
    revs = int(request.GET['revs'])
    timeline_list = {'error': 'None', 'timelines': []}
    results = Result.objects.filter(project=request.GET['proj']).filter(environment=request.GET['env'])

    dbs = request.GET['db'].split(',')
    results = filter(lambda x: x.db_conf.db_type in dbs, results)

    benchmarks = []
    if request.GET['ben'] == 'grid':
        benchmarks = ExperimentConf.BENCHMARK_TYPES
        revs = 10
    elif request.GET['ben'] == 'show_none':
        benchmarks = []
    else:
        benchmarks = [request.GET['ben']]

    results = filter(lambda x: x.benchmark_conf.benchmark_type in benchmarks, results)

    for bench in benchmarks:
        b_r = filter(lambda x: x.benchmark_conf.benchmark_type == bench, results)
        if len(b_r) == 0:
            continue

        timeline = {
            'benchmark':             bench,
            'units':                 's',
            'lessisbetter':          '(less is better)',
            'branches':              {},
            'baseline':              "None",
        }

        timeline['branches']['branch'] = {}

        for db in dbs:
            out = []
            d_r = filter(lambda x: x.db_conf.db_type == db, b_r)
            d_r = d_r[-revs:]
            for res in d_r:
                out.append(
                    [
                        res.timestamp.strftime("%Y-%m-%d %H:%M:%S"), res.throughput, "",
                        str(res.pk)
                    ]
                )

            if len(out) > 0:
                timeline['branches']['branch'][db] = out

        timeline_list['timelines'].append(timeline)

    return HttpResponse(json.dumps(timeline_list), mimetype='application/json')
