Website
=======

OLTP-Bench Website is an intermediate between the client's database and OtterTune (DBMS Auto-tuning system). 

## Requirements

##### Ubuntu Packages

```
sudo apt-get install python-pip python-dev python-mysqldb rabbitmq-server
```

##### Python Packages

```
sudo pip install -r requirements.txt
```

## Installation Instructions

##### 1. Clone the repository

```
git clone https://github.com/oltpbenchmark/website.git
```

##### 2. Update the Django settings

Navigate to the settings directory:

```
cd website/settings
```

Copy the credentials template:

```
cp credentials_TEMPLATE.py credentials.py
```

Edit `credentials.py` and update the secret key and database information.

##### 3. Create the MySQL database if it does not already exist

```
mysqladmin create -u <username> -p ottertune
```

##### 4. Migrate the Django models into the database

```
python manage.py makemigrations website
python manage.py migrate website
```

##### 5. Create the super user

```
python manage.py createsuperuser
```
    
##### 6. Preload the static database data

```
python manage.py loaddata ./script/preload/*
```
    
##### 7. Start the message broker, celery worker, and website server

```
sudo rabbitmq-server -detached
python manage.py celery worker --loglevel=info
python manage.py runserver 0.0.0.0:8000
```
