'''
Admin tasks

@author: dvanaken
'''

from collections import namedtuple
from fabric.api import env, local, quiet, settings, task
from fabric.state import output as fabric_output

from website.settings import LOG_DIR, UPLOAD_DIR

# Fabric environment settings
env.hosts = ['localhost']
fabric_output.update({
    'running' : False,
    'stdout'  : True,
})

Status = namedtuple('Status', ['RUNNING', 'STOPPED'])
STATUS = Status(0, 1)

# Supervisor setup and base commands
SUPERVISOR_CONFIG = '-c config/supervisord.conf'
SUPERVISOR_CMD = 'supervisorctl ' + SUPERVISOR_CONFIG

@task
def check_requirements():
    # Make sure supervisor is initialized
    with settings(warn_only=True):
        local('supervisord ' + SUPERVISOR_CONFIG)

# Always check requirements
check_requirements()

@task
def start_rabbitmq(detached=True):
    detached = parse_bool(detached)
    cmd = 'sudo rabbitmq-server' + (' -detached' if detached else '')
    local(cmd)

@task
def stop_rabbitmq():
    #sudo('rabbitmqctl stop', pty=False)
    local('sudo rabbitmqctl stop')

@task
def status_rabbitmq():
    with settings(warn_only=True), quiet():
        #res = sudo('rabbitmqctl status', pty=False)
        res = local('sudo rabbitmqctl status')
    if res.return_code == 2 or res.return_code == 69:
        status = STATUS.STOPPED
    elif res.return_code == 0:
        status = STATUS.RUNNING
    else:
        raise Exception("Rabbitmq: unknown status " + str(res.return_code))
    print status
    print_status(status, 'rabbitmq')
    return status
    
@task
def start_celery(detached=True):
    if status_rabbitmq() == STATUS.STOPPED:
        start_rabbitmq()
    detached = parse_bool(detached)
    if detached:
        local(SUPERVISOR_CMD + ' start celeryd')
    else:
        local('python manage.py celery worker -l info')
    
@task
def stop_celery():
    local(SUPERVISOR_CMD + ' stop celeryd')

@task
def status_celery():
    res = local(SUPERVISOR_CMD + ' status celeryd | tr -s \' \' | cut -d \' \' -f2',
                capture=True)
    try:
        status = STATUS._asdict()[res.stdout]
    except KeyError as e:
        if res.stdout == 'STARTING':
            status = STATUS.RUNNING
        elif res.stdout == 'FATAL':
            status = STATUS.STOPPED
        else:
            raise e
    print_status(status, 'celery')
    return status

@task
def start_server():
    if status_celery() == STATUS.STOPPED:
        start_celery()
    local('python manage.py runserver 0.0.0.0:8000')

@task
def stop_all():
    # TODO: update stop server
    stop_celery()
    stop_rabbitmq()


def parse_bool(value):
    if isinstance(value, bool):
        return value
    elif isinstance(value, str):
        return value.lower() == 'true'
    else:
        raise Exception('Cannot convert {} to bool'.format(type(value)))

def print_status(status, task_name):
    print "{} status: {}".format(task_name, STATUS._fields[STATUS.index(status)])

@task
def recreate_website_dbms():
    from website.settings import DATABASES

    user = DATABASES['default']['USER']
    passwd = DATABASES['default']['PASSWORD']
    name = DATABASES['default']['NAME']
    local("mysql -u {} -p{} -N -B -e \"DROP DATABASE IF EXISTS {}\"".format(user, passwd, name))
    local("mysql -u {} -p{} -N -B -e \"CREATE DATABASE {}\"".format(user, passwd, name))
    local('rm -rf ./website/migrations/')
    local('python manage.py makemigrations website')
    local('python manage.py migrate website')
    local('python manage.py migrate')
    local('python manage.py loaddata script/preload/*')
    local("mysql -u {} -p{} -D {} -N -B -e \"ALTER TABLE website_oltpbench_info MODIFY raw LONGTEXT CHARACTER SET utf8 COLLATE utf8_general_ci;\"".format(user, passwd, name))

