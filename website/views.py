from django.core.context_processors import csrf
from django.shortcuts import render_to_response, redirect, render
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.http import HttpResponse
import json
import random
import string
from django.views.decorators.csrf import csrf_exempt
from models import UserProfile, Result, Project, Target, Benchmark, Environment, Statistics, NewResultForm
from django.utils.timezone import now


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
    user.profile = UserProfile()
    user.profile.save()
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
            return HttpResponse(str(form))
        try:
            project = Project.objects.get(upload_code=form.cleaned_data['upload_code'])
        except Project.DoesNotExist:
            return HttpResponse(str(form))

        handle_result_file(project, form, request.FILES['data'])
        return HttpResponse("Succeed\n")
    return HttpResponse("POST please\n")


def handle_result_file(proj, form, file_data):
    user = proj.user

    data = "".join(map(lambda x: str(x), file_data.chunks()))
    lines = data.split("\n")
    conf_cnt = int(lines[0])
    bench_cnt = int(lines[1])

    conf_lines = lines[3:3 + conf_cnt]
    conf = []
    for line in conf_lines:
        ele = line.split(":")
        key = ele[0]
        value = ""
        if len(ele) > 1:
            value = ele[1]
        conf.append([key, value])
    conf_str = json.dumps(conf)

    try:
        targets = Target.objects.filter(configuration=conf_str)
        if len(targets) < 1:
            raise Target.DoesNotExist
        target = targets[0]
    except Target.DoesNotExist:
        target = Target()
        target.name = 'new target'
        target.configuration = conf_str
        target.project = proj
        target.save()
        target.name = 'Target#' + str(target.pk)
        target.save()

    bench_lines = lines[3 + conf_cnt: 3 + conf_cnt + bench_cnt]
    bench_str = "".join(bench_lines)
    try:
        benchs = Benchmark.objects.filter(configuration=bench_str)
        if len(benchs) < 1:
            raise Benchmark.DoesNotExist
        bench = benchs[0]
    except Benchmark.DoesNotExist:
        bench = Benchmark()
        bench.name = 'new bench conf'
        bench.user = user
        bench.configuration = bench_str
        bench.save()
        bench.name = 'BenchConf#' + str(bench.pk)
        bench.save()

    env = proj.environment

    result = Result()
    result.target = target
    result.benchmark = bench
    result.environment = env
    result.project = proj
    result.timestamp = now()
    result.save()

    for line in lines[3 + conf_cnt + bench_cnt + 1:-1]:
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


@login_required(login_url='/login/')
def target_configuration(request):
    conf_str = Target.objects.get(pk=request.GET['id']).configuration
    conf = json.loads(conf_str, encoding="UTF-8")
    context = {'parameters': conf}
    return render(request, 'target_conf.html', context)


@login_required(login_url='/login/')
def benchmark_configuration(request):
    conf_str = Benchmark.objects.get(pk=request.GET['id']).configuration
    return HttpResponse(conf_str, content_type="application/xml")


@login_required(login_url='/login/')
def result(request):
    fields = []
    for member in Statistics.PLOTTABLE_FIELDS:
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
