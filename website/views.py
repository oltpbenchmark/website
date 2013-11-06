from django.core.context_processors import csrf
from django.shortcuts import render_to_response, redirect
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.http import HttpResponse
import math
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
    user.profile.upload_code = upload_code_generator(size=20)
    print user.profile.upload_code
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
    user = User.objects.get(username=request.user.username)
    user.profile.upload_code = upload_code_generator(size=20)
    user.profile.save()
    return redirect("/")


@login_required(login_url='/login/')
def home(request):
    context = {"projects": Project.objects.filter(user=request.user),
               "environments": Environment.objects.filter(user=request.user),
               "user": request.user,
               "upload_code": request.user.profile.upload_code}
    context.update(csrf(request))
    return render_to_response('home.html', context)


@login_required(login_url='/login/')
def environment(request):
    context = {'environment': Environment.objects.get(pk=request.GET['id'])}
    context.update(csrf(request))
    return render_to_response('environment.html', context)


@login_required(login_url='/login/')
def update_env(request):
    env = Environment.objects.get(pk=request.POST['id'])
    env.name = request.POST['name']
    env.description = request.POST['description']
    env.save()
    return redirect('/')


@login_required(login_url='/login/')
def new_env(request):
    e = Environment()
    e.user = request.user
    e.name = request.POST['name']
    e.description = request.POST['description']
    e.creation_time = now()
    e.save()
    return redirect('/')


@login_required(login_url='/login/')
def delete_env(request):
    map(lambda x: Environment.objects.get(pk=x).delete(), request.POST.get('environments', []))
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
               'upload_code': request.user.profile.upload_code,
               'environments': Environment.objects.filter(user=request.user)}
    context.update(csrf(request))
    return render_to_response('project.html', context)


@login_required(login_url='/login/')
def new_project(request):
    p = Project()
    p.user = request.user
    p.name = request.POST['name']
    p.description = request.POST['description']
    p.creation_time = now()
    p.last_update = now()
    p.save()
    return redirect('/')


@login_required(login_url='/login/')
def delete_project(request):
    map(lambda x: Project.objects.get(pk=x).delete(), request.POST.get('projects', []))
    return redirect('/')


@login_required(login_url='/login/')
def update_project(request):
    p = Project.objects.get(pk=request.POST['id'])
    p.name = request.POST['name']
    p.description = request.POST['description']
    p.last_update = now()
    p.save()
    return redirect('/project/?id=' + request.POST['id'])


@csrf_exempt
def new_result(request):
    if request.method == 'POST':
        form = NewResultForm(request.POST, request.FILES)
        if not form.is_valid():
            return HttpResponse(str(form))
        users = UserProfile.objects.filter(upload_code=form.cleaned_data['upload_code'])
        if len(users) != 1:
            return HttpResponse(str(form))
        handle_result_file(users[0].user, form, request.FILES['data'])
        return HttpResponse("Succeed\n")
    return HttpResponse("POST please\n")


def handle_result_file(user, form, file_data):
    data = "".join(map(lambda x: str(x), file_data.chunks()))
    lines = data.split("\n")
    conf_cnt = int(lines[0])
    bench_cnt = int(lines[1])

    proj = Project.objects.get(pk=form.cleaned_data['project_id'])

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
    if form.cleaned_data['create_target']:
        target = Target()
        target.name = form.cleaned_data['target_name']
        target.configuration = conf_str
        target.project = proj
        target.save()
    else:
        target = Target.objects.filter(configuration=conf_str)[0]

    bench_lines = lines[3 + conf_cnt: 3 + conf_cnt + bench_cnt]
    bench_str = "".join(bench_lines)
    if form.cleaned_data['create_benchmark']:
        bench = Benchmark()
        bench.name = form.cleaned_data['benchmark_name']
        bench.user = user
        bench.configuration = bench_str
        bench.save()
    else:
        bench = Benchmark.objects.filter(configuration=bench_str)[0]

    env = Environment.objects.get(pk=form.cleaned_data['environment_id'])

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
    return render_to_response('target_conf.html', context)


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
    return render_to_response('result.html', context)


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
