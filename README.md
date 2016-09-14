Website
=======

OLTP-Bench Website is an intermediate between the client's database and OtterTune (DBMS Auto-tuning system)    

Dependencies
========
* Django == 1.9  (currently does not support Django 1.10) 
* Python +2.7
* python-mysqldb
sudo apt-get install python-mysqldb
* django-debug-toolbar +1.5
   sudo pip install django-debug-toolbar
* celery +3.1.23
    sudo pip install celery
* django-celery +3.1.17
   sudo pip install django-celery
* rabbitmq-server 
   sudo apt-get install rabbitmq-server
* Tensorflow
* sklearn 
      sudo apt-get install python-sklearn
* poster
     sudo pip install poster
* numpy
      sudo pip install numpy

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

    
