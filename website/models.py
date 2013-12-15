# -*- coding: utf-8 -*-
import re
from django.db import models
from django.contrib.auth.models import User
from django import forms


class NewResultForm(forms.Form):
    upload_code = forms.CharField(max_length=30)
    sample_data = forms.FileField()
    raw_data = forms.FileField()
    db_conf_data = forms.FileField()
    benchmark_conf_data = forms.FileField()
    summary_data = forms.FileField()


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


FEATURED_VARS = {
    'DB2': [],
    'MYSQL': [
        re.compile(ur'innodb_buffer_pool_size', re.UNICODE | re.IGNORECASE),
        re.compile(ur'innodb_buffer_pool_instances', re.UNICODE | re.IGNORECASE),
        re.compile(ur'innodb_log_file_size', re.UNICODE | re.IGNORECASE),
        re.compile(ur'innodb_log_buffer_size', re.UNICODE | re.IGNORECASE),
        re.compile(ur'innodb_flush_log_at_trx_commit', re.UNICODE | re.IGNORECASE),
        re.compile(ur'innodb_thread_concurrency', re.UNICODE | re.IGNORECASE),
        re.compile(ur'innodb_file_per_table', re.UNICODE | re.IGNORECASE),
        re.compile(ur'key_buffer_size', re.UNICODE | re.IGNORECASE),
        re.compile(ur'table_cache', re.UNICODE | re.IGNORECASE),
        re.compile(ur'thread_cache', re.UNICODE | re.IGNORECASE),
        re.compile(ur'query_cache_size', re.UNICODE | re.IGNORECASE),
    ],
    'POSTGRES': [],
    'ORACLE': [],
    'SQLSERVER': [],
    'SQLITE': [],
    'AMAZONRDS': [],
    'HSTORE': [],
    'SQLAZURE': [],
    'ASSCLOWN': [],
    'HSQLDB': [],
    'H2': [],
    'NUODB': []
}


LEARNING_VARS = {
    'DB2': [],
    'MYSQL': [
        re.compile(ur'innodb_buffer_pool_size', re.UNICODE | re.IGNORECASE),
        re.compile(ur'innodb_buffer_pool_instances', re.UNICODE | re.IGNORECASE),
        re.compile(ur'innodb_log_file_size', re.UNICODE | re.IGNORECASE),
        re.compile(ur'innodb_log_buffer_size', re.UNICODE | re.IGNORECASE),
        re.compile(ur'innodb_thread_concurrency', re.UNICODE | re.IGNORECASE),
    ],
    'POSTGRES': [],
    'ORACLE': [],
    'SQLSERVER': [],
    'SQLITE': [],
    'AMAZONRDS': [],
    'HSTORE': [],
    'SQLAZURE': [],
    'ASSCLOWN': [],
    'HSQLDB': [],
    'H2': [],
    'NUODB': []
}

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
    most_similar = models.CommaSeparatedIntegerField(max_length=100)

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

