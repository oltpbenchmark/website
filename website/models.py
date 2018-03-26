# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import re
from django.contrib.auth.models import User
from django.core.validators import validate_comma_separated_integer_list
from django.db import models
from django.utils.encoding import python_2_unicode_compatible
from django import forms


class NewResultForm(forms.Form):
    upload_code = forms.CharField(max_length=30)
    sample_data = forms.FileField()
    raw_data = forms.FileField()
    db_parameters_data = forms.FileField()
    benchmark_conf_data = forms.FileField()
    summary_data = forms.FileField()


class Project(models.Model):
    user = models.ForeignKey(User, on_delete=models.PROTECT)
    name = models.CharField(max_length=64)
    description = models.TextField()
    creation_time = models.DateTimeField()
    last_update = models.DateTimeField()

    upload_code = models.CharField(max_length=30)

    def delete(self, using=None, keep_parents=False):
        targets = DBConf.objects.filter(project=self)
        results = Result.objects.filter(project=self)
        for target in targets:
            target.delete()
        for result in results:
            result.delete()
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

    project = models.ForeignKey(Project, on_delete=models.CASCADE)
    name = models.CharField(max_length=50)
    description = models.CharField(max_length=512)
    configuration = models.TextField()
    benchmark_type = models.CharField(
        max_length=sum(len(x) + 1 for x in BENCHMARK_TYPES)
    )
    creation_time = models.DateTimeField()
    isolation = models.TextField()
    scalefactor = models.TextField()
    terminals = models.TextField()

    FILTER_FIELDS = [
        {'field': 'isolation', 'print': 'Isolation Level'},
        {'field': 'scalefactor', 'print': 'Scale Factor'},
        {'field': 'terminals', 'print': '# of Terminals'},
    ]


_MYSQL_RE_FLAGS = re.UNICODE | re.IGNORECASE


FEATURED_VARS = {
    'DB2': [],
    'MYSQL': [
        re.compile(r'innodb_buffer_pool_size', _MYSQL_RE_FLAGS),
        re.compile(r'innodb_buffer_pool_instances', _MYSQL_RE_FLAGS),
        re.compile(r'innodb_log_file_size', _MYSQL_RE_FLAGS),
        re.compile(r'innodb_log_buffer_size', _MYSQL_RE_FLAGS),
        re.compile(r'innodb_flush_log_at_trx_commit', _MYSQL_RE_FLAGS),
        re.compile(r'innodb_thread_concurrency', _MYSQL_RE_FLAGS),
        re.compile(r'innodb_file_per_table', _MYSQL_RE_FLAGS),
        re.compile(r'key_buffer_size', _MYSQL_RE_FLAGS),
        re.compile(r'table_cache', _MYSQL_RE_FLAGS),
        re.compile(r'thread_cache', _MYSQL_RE_FLAGS),
        re.compile(r'query_cache_size', _MYSQL_RE_FLAGS),
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
    'NUODB': [],
    'PELOTON': [],
}


LEARNING_VARS = {
    'DB2': [],
    'MYSQL': [
        re.compile(r'innodb_buffer_pool_size', _MYSQL_RE_FLAGS),
        re.compile(r'innodb_buffer_pool_instances', _MYSQL_RE_FLAGS),
        re.compile(r'innodb_log_file_size', _MYSQL_RE_FLAGS),
        re.compile(r'innodb_log_buffer_size', _MYSQL_RE_FLAGS),
        re.compile(r'innodb_thread_concurrency', _MYSQL_RE_FLAGS),
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
    'NUODB': [],
    'PELOTON': [],
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
        'NUODB',
        'PELOTON',
    ])

    project = models.ForeignKey(Project, on_delete=models.CASCADE)
    name = models.CharField(max_length=50)
    description = models.CharField(max_length=512)
    creation_time = models.DateTimeField()
    configuration = models.TextField()
    similar_conf = models.TextField(default="zbh")
    db_type = models.CharField(max_length=max(len(x) for x in DB_TYPES))


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
    'throughput': {
        'unit': 'transactions/second',
        'lessisbetter': False,
        'scale': 1,
        'print': 'Throughput'
        },
    'p99_latency': {
        'unit': 'milisecond',
        'lessisbetter': True,
        'scale': 0.001,
        'print': '99% Latency'
        },
    'p95_latency': {
        'unit': 'milisecond',
        'lessisbetter': True,
        'scale': 0.001,
        'print': '95% Latency'
        },
    'p90_latency': {
        'unit': 'milisecond',
        'lessisbetter': True,
        'scale': 0.001,
        'print': '90% Latency'
        },
    'p75_latency': {
        'unit': 'milisecond',
        'lessisbetter': True,
        'scale': 0.001,
        'print': '75% Latency'
        },
    'p50_latency': {
        'unit': 'milisecond',
        'lessisbetter': True,
        'scale': 0.001,
        'print': 'Med. Latency'
        },
    'p25_latency': {
        'unit': 'milisecond',
        'lessisbetter': True,
        'scale': 0.001,
        'print': '25% Latency'
        },
    'min_latency': {
        'unit': 'milisecond',
        'lessisbetter': True,
        'scale': 0.001,
        'print': 'Min Latency'
        },
    'avg_latency': {
        'unit': 'milisecond',
        'lessisbetter': True,
        'scale': 0.001,
        'print': 'Avg. Latency'
        },
    'max_latency': {
        'unit': 'milisecond',
        'lessisbetter': True,
        'scale': 0.001,
        'print': 'Max Latency'
        },
}


@python_2_unicode_compatible
class Result(models.Model):
    project = models.ForeignKey(Project, on_delete=models.CASCADE)
    benchmark_conf = models.ForeignKey(
        ExperimentConf, on_delete=models.CASCADE)
    db_conf = models.ForeignKey(DBConf, on_delete=models.CASCADE)
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
    most_similar = models.CharField(
        validators=[validate_comma_separated_integer_list],
        max_length=100)

    def __str__(self):
        return self.pk


class Statistics(models.Model):
    result = models.ForeignKey(Result, on_delete=models.CASCADE)
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
