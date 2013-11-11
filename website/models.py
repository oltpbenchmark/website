# -*- coding: utf-8 -*-
from django.db import models
from django.contrib.auth.models import User
from django import forms


class NewResultForm(forms.Form):
    upload_code = forms.CharField(max_length=30)
    data = forms.FileField()


class Environment(models.Model):
    user = models.ForeignKey(User)
    name = models.CharField(max_length=50)
    description = models.CharField(max_length=500)
    creation_time = models.DateTimeField()

    def delete(self, using=None):
        results = Result.objects.filter(environment=self)
        for r in results:
            r.delete()
        super(Environment, self).delete(using)

    def __unicode__(self):
        return u'%s: %s' % (self.user, self.name)


class Project(models.Model):
    user = models.ForeignKey(User)
    environment = models.ForeignKey(Environment)
    name = models.CharField(max_length=50)
    description = models.CharField(max_length=500)
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
    configuration = models.TextField()
    db_type = models.CharField(max_length=max(map(lambda x: len(x), DB_TYPES)))


PLOTTABLE_FIELDS = ['throughput', 'avg_latency', 'min_latency', 'p25_latency',
                    'p50_latency', 'p75_latency', 'p90_latency', 'p95_latency',
                    'p99_latency', 'max_latency']


class Result(models.Model):
    project = models.ForeignKey(Project)
    environment = models.ForeignKey(Environment)
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
    throughput = models.FloatField()


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

