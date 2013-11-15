# -*- coding: utf-8 -*-
from django.db import models
from django.contrib.auth.models import User
from django import forms


class NewResultForm(forms.Form):
    upload_code = forms.CharField(max_length=30)
    data = forms.FileField()


class Project(models.Model):
    user = models.ForeignKey(User)
    name = models.CharField(max_length=64)
    description = models.TextField()
    creation_time = models.DateTimeField()
    last_update = models.DateTimeField()

    upload_code = models.CharField(max_length=30)

    def delete(self, using=None):
        targets = DBConf.objects.filter(project=self)
        results = Result.objects.filter(project=self)
        for t in targets:
            t.delete()
        for r in results:
            r.delete()
        super(Project, self).delete(using)


class ExperimentConf(models.Model):
    BENCHMARK_TYPES = [x.upper() for x in sorted([
        'tpcc',
        'tatp',
        'wikipedia',
        'resourcestresser',
        'twitter',
        'epinions',
        'ycsb',
        'jpab',
        'seats',
        'auctionmark',
        'chbenchmark',
        'voter',
        'linkbench',
        'sibench'
    ])]

    project = models.ForeignKey(Project)
    name = models.CharField(max_length=50)
    description = models.CharField(max_length=512)
    configuration = models.TextField()
    benchmark_type = models.CharField(max_length=sum(map(lambda x: len(x) + 1, BENCHMARK_TYPES)))
    creation_time = models.DateTimeField()
    isolation = models.TextField()
    scalefactor = models.TextField()
    terminals = models.TextField()

    FILTER_FIELDS = [
        {'field': 'isolation', 'print': 'Isolation Level'},
        {'field': 'scalefactor', 'print': 'Scale Factor'},
        {'field': 'terminals', 'print': '# of Terminals'},
    ]


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

    project = models.ForeignKey(Project)
    name = models.CharField(max_length=50)
    description = models.CharField(max_length=512)
    creation_time = models.DateTimeField()
    configuration = models.TextField()
    db_type = models.CharField(max_length=max(map(lambda x: len(x), DB_TYPES)))


PLOTTABLE_FIELDS = [
    {'field': 'throughput', 'print': 'Throughput'},
    {'field': 'p99_latency', 'print': '99% Latency'},
    {'field': 'p95_latency', 'print': '95% Latency'},
    {'field': 'p90_latency', 'print': '90% Latency'},
    {'field': 'avg_latency', 'print': 'Avg. Latency'},
    {'field': 'p50_latency', 'print': 'Med. Latency'},
    {'field': 'max_latency', 'print': 'Max Latency'},
    {'field': 'p75_latency', 'print': '75% Latency'},
    {'field': 'p25_latency', 'print': '25% Latency'},
    {'field': 'min_latency', 'print': 'Min Latency'},
]

METRIC_META = {
    'throughput': {'unit': 'transactions/second', 'lessisbetter': False, 'scale': 1},
    'p99_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 1000},
    'p95_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 1000},
    'p90_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 1000},
    'p75_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 1000},
    'p50_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 1000},
    'p25_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 1000},
    'min_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 1000},
    'avg_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 1000},
    'max_latency': {'unit': 'milisecond', 'lessisbetter': True, 'scale': 1000},
}

class Result(models.Model):
    project = models.ForeignKey(Project)
    benchmark_conf = models.ForeignKey(ExperimentConf)
    db_conf = models.ForeignKey(DBConf)
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

