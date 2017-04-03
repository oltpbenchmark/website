'''
Admin tasks

@author: dvanaken
'''

from collections import namedtuple
from fabric.api import env, local, quiet, settings, sudo, task
from fabric.state import output as fabric_output

# Fabric environment settings
env.hosts = ['localhost']
fabric_output.update({
    'running' : False,
    'stdout'  : True,
})

# Supervisor setup and base commands
SUPERVISOR_CONFIG = '-c config/supervisord.conf'
SUPERVISOR_CMD = 'supervisorctl ' + SUPERVISOR_CONFIG
with settings(warn_only=True), quiet():
    # Make sure supervisor is initialized
    local('supervisord ' + SUPERVISOR_CONFIG)

Status = namedtuple('Status', ['RUNNING', 'STOPPED'])
STATUS = Status(0, 1)

@task
def start_rabbitmq(detached=True):
    detached = parse_bool(detached)
    cmd = 'rabbitmq-server' + (' -detached' if detached else '')
    sudo(cmd, pty=False)

@task
def stop_rabbitmq():
    sudo('rabbitmqctl stop', pty=False)

@task
def status_rabbitmq():
    with settings(warn_only=True), quiet():
        res = sudo('rabbitmqctl status', pty=False)
    if res.return_code == 2:
        status = STATUS.STOPPED
    elif res.return_code == 0:
        status = STATUS.RUNNING
    else:
        raise Exception("Rabbitmq: unknown status " + res.return_code)
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
    