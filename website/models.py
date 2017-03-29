# -*- coding: utf-8 -*-
import re
from django.db import models
from django.contrib.auth.models import User
from django import forms
from django.contrib import admin


class Knob_catalog(models.Model):
    id = models.IntegerField(primary_key=True)
#    dbms_name = models.TextField()
#    dbms_version = models.TextField()
    dbms_id = models.IntegerField()
    name = models.TextField()
    vartype = models.TextField()
    unit = models.TextField(null=True)
    category = models.TextField(null=True)
    summary = models.TextField(null=True)
    description = models.TextField(null=True)
    scope = models.TextField()
    dynamic = models.BooleanField()
    min_value = models.TextField(null=True)
    max_value = models.TextField(null=True)
    valid_vals = models.TextField(null=True) 
    default_val = models.TextField()
    deprecated = models.BooleanField()
    dangerous = models.TextField(null=True)
    safe_vals = models.TextField(null=True)
    rank = models.IntegerField(null=True)
    

class Metric_catalog(models.Model):
    id = models.IntegerField(primary_key=True)
    dbms_id = models.IntegerField()
 #   dbms_name = models.TextField()
 #   dbms_version = models.TextField()
    scale = models.TextField()
    name = models.TextField()
    vartype = models.TextField()
    description = models.TextField(null=True)
    scope = models.TextField(null=True)
    featured = models.BooleanField()

class DBMS_catalog(models.Model): 
    id = models.IntegerField(primary_key=True)
    dbms_name = models.TextField()
    version = models.TextField()


class Workload_info(models.Model):
    #id = models.AutoField(primary_key=True) 
    isolation = models.CharField(max_length=64)
    scalefactor = models.IntegerField()
    terminals = models.IntegerField()
    time = models.IntegerField()
    rate = models.CharField(max_length=64)
    skew = models.FloatField(null=True)
    trans_weights = models.TextField()
    workload = models.TextField()

class Oltpbench_info(models.Model):
#    id = models.IntegerField(primary_key=True)

    user = models.ForeignKey(User) 
    dbms_name = models.CharField(max_length=64)
    dbms_version = models.CharField(max_length=64)

    hardware = models.CharField(null=True, max_length=64)
    cluster = models.TextField(null=True)

    summary = models.TextField()
    res = models.TextField()
    status = models.TextField()
    cfg = models.TextField()
    raw = models.TextField(null=True)
    
    wid = models.ForeignKey(Workload_info)
     

class NewResultForm(forms.Form):
    upload_code = forms.CharField(max_length=30)

    upload_use = forms.CharField(max_length=30) #compute/store
    hardware = forms.CharField(max_length=30) # hardware 
    cluster = forms.CharField(max_length=200) # store cluster    

    sample_data = forms.FileField()
    raw_data = forms.FileField()
    db_conf_data = forms.FileField()
    benchmark_conf_data = forms.FileField()
    summary_data = forms.FileField()
    db_status =forms.FileField() #.status



class Project(models.Model):
    user = models.ForeignKey(User)
    name = models.CharField(max_length=64)
    description = models.TextField()
    creation_time = models.DateTimeField()
    last_update = models.DateTimeField()

    upload_code = models.CharField(max_length=30)

    def delete(self, using=None):
        apps = Application.objects.filter(project=self)
        for x in apps:
	    x.delete()
        super(Project, self).delete(using)




class Task(models.Model):
    id = models.IntegerField(primary_key=True)
    creation_time = models.DateTimeField()
    finish_time = models.DateTimeField(null=True)
    running_time = models.IntegerField(null=True)
    status = models.CharField(max_length=64) 
    traceback =  models.TextField(null=True)
    result = models.TextField(null=True)


class Application(models.Model):
    user = models.ForeignKey(User)
    name = models.CharField(max_length=64)
    description = models.TextField()
   
    project = models.ForeignKey(Project)
    creation_time = models.DateTimeField()
    last_update = models.DateTimeField()

    upload_code = models.CharField(max_length=30)
  
    def delete(self, using=None):
        targets = DBConf.objects.filter(application=self)
        results = Result.objects.filter(application=self)
        expconfs =  ExperimentConf.objects.filter(application=self)
        for t in targets:
            t.delete()
        for r in results:
            r.delete()
        for x in expconfs:
            x.delete()
        super(Application, self).delete(using)





class ExperimentConf(models.Model):
    application = models.ForeignKey(Application)
   

    name = models.CharField(max_length=50)
    description = models.CharField(max_length=512)
    configuration = models.TextField()
    benchmark_type = models.CharField(max_length=512)
    creation_time = models.DateTimeField()
    isolation = models.TextField()
    scalefactor = models.TextField()
    terminals = models.TextField()

    FILTER_FIELDS = [
        {'field': 'isolation', 'print': 'Isolation Level'},
        {'field': 'scalefactor', 'print': 'Scale Factor'},
        {'field': 'terminals', 'print': '# of Terminals'},
    ]



class FEATURED_PARAMS(models.Model):
    db_type = models.CharField(max_length=64)
    params = models.CharField(max_length=512)

class Website_Conf(models.Model):
    name = models.CharField(max_length=64)
    value = models.CharField(max_length=512)

class LEARNING_PARAMS(models.Model):
    db_type = models.CharField(max_length=64)
    params = models.CharField(max_length=512)

class KNOB_PARAMS(models.Model):
    db_type = models.CharField(max_length=64)
    params = models.CharField(max_length=512)


class DBConf(models.Model):
    DB_TYPES = sorted([
        'DB2',
        'MYSQL',
        'POSTGRES',
        'ORACLE',
        'SQLSERVER',
        'SQLITE',
        'AMAZONRDS',
        'HSTORE',
        'SQLAZURE',
        'ASSCLOWN',
        'HSQLDB',
        'H2',
        'NUODB'
    ])
    application = models.ForeignKey(Application)
    name = models.CharField(max_length=50)
    description = models.CharField(max_length=512)
    creation_time = models.DateTimeField()
    configuration = models.TextField()
    similar_conf = models.TextField(default = "zbh")
    db_type = models.CharField(max_length=max(map(lambda x: len(x), DB_TYPES)))


PLOTTABLE_FIELDS = [
    'throughput',
    'p99_latency',
    'p95_latency',
    'p90_latency',
    'avg_latency',
    'p50_latency',
    'max_latency',
    'p75_latency',
    'p25_latency',
    'min_latency'
]

METRIC_META = {
    'throughput': {'unit': 'transactions/second', 'lessisbetter': False, 'scale': 1, 'print': 'Throughput'},
    'p99_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 0.001, 'print': '99% Latency'},
    'p95_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 0.001, 'print': '95% Latency'},
    'p90_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 0.001, 'print': '90% Latency'},
    'p75_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 0.001, 'print': '75% Latency'},
    'p50_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 0.001, 'print': 'Med. Latency'},
    'p25_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 0.001, 'print': '25% Latency'},
    'min_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 0.001, 'print': 'Min Latency'},
    'avg_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 0.001, 'print': 'Avg. Latency'},
    'max_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 0.001, 'print': 'Max Latency'}
}


class Result(models.Model):
    benchmark_conf = models.ForeignKey(ExperimentConf)
    db_conf = models.ForeignKey(DBConf)

    application = models.ForeignKey(Application)
    creation_time = models.DateTimeField()

    timestamp = models.DateTimeField()
    throughput = models.FloatField()
    avg_latency = models.FloatField()
    min_latency = models.FloatField()
    p25_latency = models.FloatField()
    p50_latency = models.FloatField()
    p75_latency = models.FloatField()
    p90_latency = models.FloatField()
    p95_latency = models.FloatField()
    p99_latency = models.FloatField()
    max_latency = models.FloatField()
    #most_similar = models.CommaSeparatedIntegerField(max_length=100)
    most_similar = models.CharField(max_length=100)

    def __unicode__(self):
        return unicode(self.pk)


class Statistics(models.Model):
    result = models.ForeignKey(Result)
    time = models.IntegerField()
    throughput = models.FloatField()
    avg_latency = models.FloatField()
    min_latency = models.FloatField()
    p25_latency = models.FloatField()
    p50_latency = models.FloatField()
    p75_latency = models.FloatField()
    p90_latency = models.FloatField()
    p95_latency = models.FloatField()
    p99_latency = models.FloatField()
    max_latency = models.FloatField()

