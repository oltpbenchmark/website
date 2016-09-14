Website
=======

OLTP-Bench Website is an intermediate between the client's database and OtterTune (DBMS Auto-tuning system)    

Dependencies
========
* Python +2.7
* [Tensorflow](https://www.tensorflow.org/versions/r0.10/get_started/os_setup.html#pip-installation)
* Django == 1.9  (currently does not support Django 1.10)
* django-debug-toolbar +1.5
* celery +3.1.23
* django-celery +3.1.17
* rabbitmq-server
* python packages:python-mysqldb,  sklearn, poster,  numpy

    sudo apt-get install python-pip python-dev python-sklearn python-mysqldb rabbitmq-server
    sudo pip install  django==1.9 numpy  poster  celery django-celery  django-debug-toolbar

Quick Start
=====
###1. First revise the setting.py file. 

  Set the BasePath of your website (line 8) 

  Set your database access (line 54) . (Name, User, Password ...) 

###2. Migrate the models into the database
    python manage.py  makemigrations website
    python manage.py  migrate
    
###3.Preload the parameters
    python manage.py  loaddata  ./preload/*
    
###4.Run the rabbitmq and celery worker
    rabbitmq-server & 
    python manage.py celery worker --loglevel=info
###5. Run the website server
    python manage.py runserver 0.0.0.0:8000

    
