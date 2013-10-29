from django.core import serializers
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
from models import UserProfile, Result, UploadFileForm


def login_view(request):
    c = {}
    c.update(csrf(request))
    return render_to_response('website/login.html', c)

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
        user = create_user(username=post['email'], email=post['email'], password=post['password'])
        return auth_and_login(request)
    else:
        return redirect("/login/")

@login_required(login_url='/login/')
def secured(request):
    context = {"username": request.user.username,
               "upload_code": User.objects.get(username=request.user.username).profile.upload_code}
    return render_to_response("website/secure.html", context)

@login_required(login_url='/login/')
def logout_view(request):
    logout(request)
    return redirect("/login/")

@login_required(login_url='/login/')
def get_data(request):
    cnt = int(request.GET["count"])
    scale = float(request.GET["scale"])
    results = []
    for i in range(0, cnt):
        results.append({'x': i/scale, 'y': math.sin(i/scale)})
    resultDict = {'key': 'Sine Wave', 'values': results, 'color': '#ff7f0e'}
    finalResults = [resultDict]
    res = json.dumps(finalResults, encoding="UTF-8")
    return HttpResponse(res, mimetype='application/json')

@login_required(login_url='/login/')
def get_new_upload_code(request):
    user = User.objects.get(username=request.user.username)
    user.profile.upload_code = upload_code_generator(size=20)
    user.profile.save()
    return redirect("/")

@csrf_exempt
def upload(request):
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        users = UserProfile.objects.filter(upload_code=request.POST['upload_code'])
        if len(users) != 1:
            return HttpResponse("Wrong")
        if form.is_valid():
            handle_uploaded_file(users[0].user, request.FILES['file'])
            return HttpResponse("Succeed")
    return HttpResponse("Wrong")

def handle_uploaded_file(user, f):
    data = Result()
    data.user = user
    data.data = "".join(map(lambda x: str(x), f.chunks()))
    data.save()

@login_required(login_url='/login/')
def get_upload_data(request):
    files = Result.objects.filter(user=request.user.id)
    result = []
    for r in files:
        result.append({'id': r.pk, 'data': r.data})
    res = json.dumps(result, encoding="UTF-8")
    return HttpResponse(res, mimetype='application/json')

@login_required(login_url='/login/')
def del_upload_data(request):
    results = Result.objects.filter(pk=request.GET['id'])
    if len(results) != 1:
        return HttpResponse("{}", mimetype='application/json')
    data = results[0]
    if data.user != request.user:
        return HttpResponse("{}", mimetype='application/json')
    data.delete()
    return HttpResponse("{}", mimetype='application/json')
