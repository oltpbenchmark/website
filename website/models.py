from collections import OrderedDict

from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.core.validators import validate_comma_separated_integer_list
from django.db import models

from .types import (DBMSType, MetricType, VarType, HardwareType,
                    KnobUnitType, PipelineTaskType, StatsType)
from .utils import JSONUtil


class DBMSCatalog(models.Model):
    type = models.IntegerField(choices=DBMSType.choices())
    version = models.CharField(max_length=16)

    @property
    def key(self):
        return '{}_{}'.format(self.name, self.version)

    @property
    def name(self):
        return DBMSType.name(self.type)

    @property
    def full_name(self):
        return '{} v{}'.format(self.name, self.version)


class KnobCatalog(models.Model):
    dbms = models.ForeignKey(DBMSCatalog)
    name = models.CharField(max_length=64)
    vartype = models.IntegerField(choices=VarType.choices())
    unit = models.IntegerField(choices=KnobUnitType.choices())
    category = models.TextField(null=True)
    summary = models.TextField(null=True)
    description = models.TextField(null=True)
    scope = models.CharField(max_length=16)
    minval = models.CharField(max_length=32, null=True)
    maxval = models.CharField(max_length=32, null=True)
    default = models.TextField()
    enumvals = models.TextField(null=True)
    context = models.CharField(max_length=32)
    tunable = models.BooleanField()


class MetricCatalog(models.Model):
    dbms = models.ForeignKey(DBMSCatalog)
    name = models.CharField(max_length=64)
    vartype = models.IntegerField(choices=VarType.choices())
    summary = models.TextField(null=True)
    scope = models.CharField(max_length=16)
    metric_type = models.IntegerField(choices=MetricType.choices())

    def clean_fields(self, exclude=None):
        super(MetricCatalog, self).clean_fields(exclude=exclude)
        if self.metric_type == MetricType.COUNTER and \
                self.vartype != VarType.INTEGER:
            raise ValidationError('Counter metrics must be integers.')


class Project(models.Model):
    user = models.ForeignKey(User)
    name = models.CharField(max_length=64)
    description = models.TextField(null=True)
    creation_time = models.DateTimeField()
    last_update = models.DateTimeField()

    def delete(self, using=None):
        apps = Application.objects.filter(project=self)
        for x in apps:
            x.delete()
        super(Project, self).delete(using)


class Hardware(models.Model):
    type = models.IntegerField(choices=HardwareType.choices())
    name = models.CharField(max_length=32)
    cpu = models.IntegerField()
    memory = models.FloatField()
    storage = models.CharField(
            max_length=64,
            validators=[validate_comma_separated_integer_list])
    storage_type = models.CharField(max_length=16)
    additional_specs = models.TextField(null=True)


class Application(models.Model):
    user = models.ForeignKey(User)
    name = models.CharField(max_length=64)
    description = models.TextField()
    hardware = models.ForeignKey(Hardware)

    project = models.ForeignKey(Project)
    creation_time = models.DateTimeField()
    last_update = models.DateTimeField()

    upload_code = models.CharField(max_length=30, unique=True)
    tuning_session = models.BooleanField()
    nondefault_settings = models.TextField(null=True)

    def delete(self, using=None):
        targets = DBConf.objects.filter(application=self)
        results = Result.objects.filter(application=self)
        expconfs = BenchmarkConfig.objects.filter(application=self)
        for t in targets:
            t.delete()
        for r in results:
            r.delete()
        for x in expconfs:
            x.delete()
        super(Application, self).delete(using)


class BenchmarkConfig(models.Model):
    application = models.ForeignKey(Application)
    name = models.CharField(max_length=64)
    description = models.CharField(max_length=512, null=True)
    configuration = models.TextField()
    benchmark_type = models.CharField(max_length=64)
    creation_time = models.DateTimeField()
    isolation = models.CharField(max_length=64)
    scalefactor = models.FloatField()
    terminals = models.IntegerField()
    time = models.IntegerField()
    rate = models.CharField(max_length=32)
    skew = models.FloatField(null=True)
    transaction_types = models.TextField(
        validators=[validate_comma_separated_integer_list])
    transaction_weights = models.TextField(
        validators=[validate_comma_separated_integer_list])

    FILTER_FIELDS = [
        {'field': 'isolation', 'print': 'Isolation Level'},
        {'field': 'scalefactor', 'print': 'Scale Factor'},
        {'field': 'terminals', 'print': '# of Terminals'},
    ]

    def clean_fields(self, exclude=None):
        super(BenchmarkConfig, self).clean_fields(exclude=exclude)
        if self.time <= 0:
            raise ValidationError('Time must be greater than 0.')


class DBConf(models.Model):
    application = models.ForeignKey(Application)
    name = models.CharField(max_length=50)
    description = models.CharField(max_length=512)
    creation_time = models.DateTimeField()
    configuration = models.TextField()
    orig_config_diffs = models.TextField()
    dbms = models.ForeignKey(DBMSCatalog)

    def get_tuning_configuration(self, return_both=False):
        config = JSONUtil.loads(self.configuration)
        param_catalog = KnobCatalog.objects.filter(dbms=self.dbms)
        tunable_params = OrderedDict()
        if return_both is True:
            other_params = OrderedDict()
        for p in param_catalog:
            if p.tunable is True:
                tunable_params[p.name] = config[p.name]
            elif return_both is True:
                other_params[p.name] = config[p.name]
        return tunable_params if return_both is False else \
            (tunable_params, other_params)


class DBMSMetrics(models.Model):
    application = models.ForeignKey(Application)
    name = models.CharField(max_length=50)
    description = models.CharField(max_length=512)
    creation_time = models.DateTimeField()
    execution_time = models.IntegerField()
    configuration = models.TextField()
    orig_config_diffs = models.TextField()
    dbms = models.ForeignKey(DBMSCatalog)

    def clean_fields(self, exclude=None):
        super(DBMSMetrics, self).clean_fields(exclude=exclude)
        if self.execution_time <= 0:
            raise ValidationError('Execution time must be greater than 0.')

    def get_numeric_configuration(self, normalize=True, return_both=False):
        config = JSONUtil.loads(self.configuration)
        metric_catalog = MetricCatalog.objects.filter(dbms=self.dbms)
        numeric_metrics = OrderedDict()
        if return_both is True:
            other_metrics = OrderedDict()
        for m in metric_catalog:
            if m.metric_type == MetricType.COUNTER:
                numeric_metrics[m.name] = \
                        float(config[m.name]) / self.execution_time if \
                        normalize is True else float(config[m.name])
            elif return_both is True:
                other_metrics[m.name] = config[m.name]
        return numeric_metrics if return_both is False else \
            (numeric_metrics, other_metrics)


class Result(models.Model):
    application = models.ForeignKey(Application)
    dbms = models.ForeignKey(DBMSCatalog)
    benchmark_config = models.ForeignKey(BenchmarkConfig)
    dbms_config = models.ForeignKey(DBConf)
    dbms_metrics = models.ForeignKey(DBMSMetrics)

    creation_time = models.DateTimeField()
    summary = models.TextField()
    samples = models.TextField()
    task_ids = models.CharField(max_length=180, null=True)
    summary_stats = models.ForeignKey('Statistics', null=True)
    timestamp = models.DateTimeField()
    most_similar = models.CharField(max_length=100, validators=[
                                    validate_comma_separated_integer_list])

    def __unicode__(self):
        return unicode(self.pk)


class WorkloadCluster(models.Model):
    dbms = models.ForeignKey(DBMSCatalog)
    hardware = models.ForeignKey(Hardware)
    cluster_name = models.CharField(max_length=128, unique=True)

    @property
    def default_name(self):
        return '{}_{}_UNASSIGNED'.format(self.dbms.pk, self.hardware.pk)

    def isdefault(self):
        return self.cluster_name == self.default_name

    @staticmethod
    def get_default_cluster(dbms, hardware):
        name = '{}_{}_UNASSIGNED'.format(dbms.pk, hardware.pk)
        default_obj = WorkloadCluster.objects.filter(
            dbms=dbms, hardware=hardware, cluster_name=name).first()
        if default_obj is None:
            default_obj = WorkloadCluster()
            default_obj.dbms = dbms
            default_obj.hardware = hardware
            default_obj.cluster_name = name
            default_obj.save()
        return default_obj


class ResultData(models.Model):
    result = models.ForeignKey(Result)
    cluster = models.ForeignKey(WorkloadCluster)
    param_data = models.TextField()
    metric_data = models.TextField()

    class Meta:
        ordering = ('cluster',)

    def clean_fields(self, exclude=None):
        super(ResultData, self).clean_fields(exclude=exclude)


class PipelineResult(models.Model):
    dbms = models.ForeignKey(DBMSCatalog)
    hardware = models.ForeignKey(Hardware)
    creation_timestamp = models.DateTimeField()
    task_type = models.IntegerField(choices=PipelineTaskType.choices())
    value = models.TextField()

    @staticmethod
    def get_latest(dbms, hardware, task_type):
        results = PipelineResult.objects.filter(
            dbms=dbms, hardware=hardware, task_type=task_type)
        return None if len(results) == 0 else results.latest()

    class Meta:
        unique_together = ("dbms", "hardware",
                           "creation_timestamp", "task_type")
        get_latest_by = ('creation_timestamp')


class StatsManager(models.Manager):
    THROUGHPUT  = 'throughput'
    P99_LATENCY = 'p99_latency'
    P95_LATENCY = 'p95_latency'
    P90_LATENCY = 'p90_latency'
    AVG_LATENCY = 'avg_latency'
    MED_LATENCY = 'p50_latency'
    MAX_LATENCY = 'max_latency'
    P75_LATENCY = 'p75_latency'
    P25_LATENCY = 'p25_latency'
    MIN_LATENCY = 'min_latency'
    TIME        = 'time'

    LATENCY_UNIT  = 'milliseconds'
    TPUT_UNIT     = 'transactions/second'
    LATENCY_SCALE = 0.001
    TPUT_SCALE    = 1

    LESS_IS_BETTER = '(less is better)'
    MORE_IS_BETTER = '(more is better)'

    THROUGHPUT_LABEL  = 'Throughput (requests/second)'
    P99_LATENCY_LABEL = '99th Percentile Latency (microseconds)'
    P95_LATENCY_LABEL = '95th Percentile Latency (microseconds)'
    P90_LATENCY_LABEL = '90th Percentile Latency (microseconds)'
    AVG_LATENCY_LABEL = 'Average Latency (microseconds)'
    MED_LATENCY_LABEL = 'Median Latency (microseconds)'
    MAX_LATENCY_LABEL = 'Maximum Latency (microseconds)'
    P75_LATENCY_LABEL = '75th Percentile Latency (microseconds)'
    P25_LATENCY_LABEL = '25th Percentile Latency (microseconds)'
    MIN_LATENCY_LABEL = 'Minimum Latency (microseconds)'
    TIME_LABEL        = 'Time (seconds)'

    DEFAULT_METRICS = [P99_LATENCY, THROUGHPUT]

    METRIC_META = OrderedDict([
        (THROUGHPUT,  {'unit': TPUT_UNIT,   'improvement': MORE_IS_BETTER, 'scale': TPUT_SCALE,    'print': THROUGHPUT_LABEL}),
        (P99_LATENCY, {'unit': LATENCY_UNIT, 'improvement': LESS_IS_BETTER, 'scale': LATENCY_SCALE, 'print': P99_LATENCY_LABEL}),
        (P95_LATENCY, {'unit': LATENCY_UNIT, 'improvement': LESS_IS_BETTER, 'scale': LATENCY_SCALE, 'print': P95_LATENCY_LABEL}),
        (P90_LATENCY, {'unit': LATENCY_UNIT, 'improvement': LESS_IS_BETTER, 'scale': LATENCY_SCALE, 'print': P90_LATENCY_LABEL}),
        (P75_LATENCY, {'unit': LATENCY_UNIT, 'improvement': LESS_IS_BETTER, 'scale': LATENCY_SCALE, 'print': P75_LATENCY_LABEL}),
        (P25_LATENCY, {'unit': LATENCY_UNIT, 'improvement': LESS_IS_BETTER, 'scale': LATENCY_SCALE, 'print': P25_LATENCY_LABEL}),
        (MIN_LATENCY, {'unit': LATENCY_UNIT, 'improvement': LESS_IS_BETTER, 'scale': LATENCY_SCALE, 'print': MIN_LATENCY_LABEL}),
        (MED_LATENCY, {'unit': LATENCY_UNIT, 'improvement': LESS_IS_BETTER, 'scale': LATENCY_SCALE, 'print': MED_LATENCY_LABEL}),
        (MAX_LATENCY, {'unit': LATENCY_UNIT, 'improvement': LESS_IS_BETTER, 'scale': LATENCY_SCALE, 'print': MAX_LATENCY_LABEL}),
        (AVG_LATENCY, {'unit': LATENCY_UNIT, 'improvement': LESS_IS_BETTER, 'scale': LATENCY_SCALE, 'print': AVG_LATENCY_LABEL}),
    ])

    def __init__(self):
        super(StatsManager, self).__init__()
        self.label_map = OrderedDict()
        for mname, md in StatsManager.METRIC_META.iteritems():
            self.label_map[md['print']] = mname
            md['print'] = md['print'].rsplit(' ', 1)[0]
        self.label_map[StatsManager.TIME_LABEL] = StatsManager.TIME

    def create_summary_stats(self, summary, result, time):
        flat_summary = summary.copy()
        flat_summary.update(summary['Latency Distribution'])
        del flat_summary['Latency Distribution']

        stats = Statistics()
        stats.data_result = result
        stats.type = StatsType.SUMMARY
        stats.time = int(time)
        for name, entry in flat_summary.iteritems():
            if name in self.label_map:
                setattr(stats, self.label_map[name], float(entry))
        stats.save()
        return stats

    def create_sample_stats(self, sample_csv, result):
        sample_lines = sample_csv.split('\n')
        header = [h.strip() for h in sample_lines[0].split(',')]
        header = [self.label_map[h] for h in header if h in self.label_map]

        all_stats = []
        for line in sample_lines[1:]:
            if line == '':
                continue
            stats = Statistics()
            stats.data_result = result
            stats.type = StatsType.SAMPLES
            entries = line.strip().split(',')
            for name, entry in zip(header, entries):
                if name == self.TIME:
                    entry = int(entry)
                else:
                    entry = float(entry)
                setattr(stats, name, entry)
            stats.save()
            all_stats.append(stats)
        return all_stats

    def get_external_metrics(self, summary):
        stats = {self.label_map[k]: float(v) for k,v in \
                 summary['Latency Distribution'].iteritems()}
        stats[self.THROUGHPUT] = float(summary[self.THROUGHPUT_LABEL])
        for mname in StatsManager.METRIC_META.keys():
            if mname not in stats:
                raise Exception('Missing external metric: {}'.format(mname))
        return stats


class Statistics(models.Model):

    data_result = models.ForeignKey(Result)
    type = models.IntegerField(choices = StatsType.choices())
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

    objects = StatsManager()
