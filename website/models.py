from collections import namedtuple, OrderedDict

import xml.dom.minidom
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.core.validators import (validate_comma_separated_integer_list,
                                    MinValueValidator)
from django.db import models
from django.utils.timezone import now

from .types import (DBMSType, LabelStyleType, MetricType, HardwareType,
                    KnobUnitType, PipelineTaskType, StatsType, VarType)


class BaseModel(object):

    @classmethod
    def get_labels(cls, style=LabelStyleType.DEFAULT_STYLE):
        from .utils import LabelUtil

        labels = {}
        fields = cls._meta.get_fields()
        for field in fields:
            try:
                verbose_name = field.verbose_name
                if field.name == 'id':
                    verbose_name = cls._model_name() + ' id'
                labels[field.name] = verbose_name
            except:
                pass
        return LabelUtil.style_labels(labels, style)

    @classmethod
    def _model_name(cls):
        return cls.__name__


class DBMSCatalog(models.Model):
    type = models.IntegerField(choices=DBMSType.choices())
    version = models.CharField(max_length=16)

    @property
    def name(self):
        return DBMSType.name(self.type)

    @property
    def key(self):
        return '{}_{}'.format(self.name, self.version)

    @property
    def full_name(self):
        return '{} v{}'.format(self.name, self.version)

    def __unicode__(self):
        return self.full_name


class KnobCatalog(models.Model, BaseModel):
    dbms = models.ForeignKey(DBMSCatalog)
    name = models.CharField(max_length=64)
    vartype = models.IntegerField(choices=VarType.choices(), verbose_name="variable type")
    unit = models.IntegerField(choices=KnobUnitType.choices())
    category = models.TextField(null=True)
    summary = models.TextField(null=True, verbose_name='description')
    description = models.TextField(null=True)
    scope = models.CharField(max_length=16)
    minval = models.CharField(max_length=32, null=True, verbose_name="minimum value")
    maxval = models.CharField(max_length=32, null=True, verbose_name="maximum value")
    default = models.TextField(verbose_name="default value")
    enumvals = models.TextField(null=True, verbose_name="valid values")
    context = models.CharField(max_length=32)
    tunable = models.BooleanField(verbose_name="tunable")


class MetricCatalog(models.Model, BaseModel):
    dbms = models.ForeignKey(DBMSCatalog)
    name = models.CharField(max_length=64)
    vartype = models.IntegerField(choices=VarType.choices())
    summary = models.TextField(null=True, verbose_name='description')
    scope = models.CharField(max_length=16)
    metric_type = models.IntegerField(choices=MetricType.choices())


class Project(models.Model, BaseModel):
    user = models.ForeignKey(User)
    name = models.CharField(max_length=64, verbose_name="project name")
    description = models.TextField(null=True, blank=True)
    creation_time = models.DateTimeField()
    last_update = models.DateTimeField()

    def delete(self, using=None):
        apps = Application.objects.filter(project=self)
        for x in apps:
            x.delete()
        super(Project, self).delete(using)

    def __unicode__(self):
        return self.name


class Hardware(models.Model):
    type = models.IntegerField(choices=HardwareType.choices())
    name = models.CharField(max_length=32)
    cpu = models.IntegerField()
    memory = models.FloatField()
    storage = models.CharField(max_length=64,
            validators=[validate_comma_separated_integer_list])
    storage_type = models.CharField(max_length=16)
    additional_specs = models.TextField(null=True)

    def __unicode__(self):
        return HardwareType.TYPE_NAMES[self.type]

MetricMeta = namedtuple('MetricMeta', ['name', 'pprint', 'unit', 'short_unit', 'scale', 'improvement'])

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

    UNIT_MILLISECONDS = ('milliseconds', 'ms')
    UNIT_TXN_PER_SEC = ('transactions/second', 'txn/sec')

    LATENCY_UNIT  = UNIT_MILLISECONDS
    TPUT_UNIT     = UNIT_TXN_PER_SEC
    LATENCY_SCALE = 0.001
    TPUT_SCALE    = 1

    LESS_IS_BETTER = '(less is better)'
    MORE_IS_BETTER = '(more is better)'

    THROUGHPUT_LABEL  = 'Throughput'
    P99_LATENCY_LABEL = '99th Percentile Latency'
    P95_LATENCY_LABEL = '95th Percentile Latency'
    P90_LATENCY_LABEL = '90th Percentile Latency'
    AVG_LATENCY_LABEL = 'Average Latency'
    MED_LATENCY_LABEL = 'Median Latency'
    MAX_LATENCY_LABEL = 'Maximum Latency'
    P75_LATENCY_LABEL = '75th Percentile Latency'
    P25_LATENCY_LABEL = '25th Percentile Latency'
    MIN_LATENCY_LABEL = 'Minimum Latency'
    TIME_LABEL        = 'Time'

    DEFAULT_METRICS = [P99_LATENCY, THROUGHPUT]

    METRIC_META = OrderedDict([
        (THROUGHPUT,  MetricMeta(THROUGHPUT, THROUGHPUT_LABEL, TPUT_UNIT[0], TPUT_UNIT[1], TPUT_SCALE, MORE_IS_BETTER)),
        (P99_LATENCY, MetricMeta(P99_LATENCY, P99_LATENCY_LABEL, LATENCY_UNIT[0], LATENCY_UNIT[1], LATENCY_SCALE, LESS_IS_BETTER)),
        (P95_LATENCY, MetricMeta(P95_LATENCY, P95_LATENCY_LABEL, LATENCY_UNIT[0], LATENCY_UNIT[1], LATENCY_SCALE, LESS_IS_BETTER)),
        (P90_LATENCY, MetricMeta(P90_LATENCY, P90_LATENCY_LABEL, LATENCY_UNIT[0], LATENCY_UNIT[1], LATENCY_SCALE, LESS_IS_BETTER)),
        (P75_LATENCY, MetricMeta(P75_LATENCY, P75_LATENCY_LABEL, LATENCY_UNIT[0], LATENCY_UNIT[1], LATENCY_SCALE, LESS_IS_BETTER)),
        (P25_LATENCY, MetricMeta(P25_LATENCY, P25_LATENCY_LABEL, LATENCY_UNIT[0], LATENCY_UNIT[1], LATENCY_SCALE, LESS_IS_BETTER)),
        (MIN_LATENCY, MetricMeta(MIN_LATENCY, MIN_LATENCY_LABEL, LATENCY_UNIT[0], LATENCY_UNIT[1], LATENCY_SCALE, LESS_IS_BETTER)),
        (MED_LATENCY, MetricMeta(MED_LATENCY, MED_LATENCY_LABEL, LATENCY_UNIT[0], LATENCY_UNIT[1], LATENCY_SCALE, LESS_IS_BETTER)),
        (MAX_LATENCY, MetricMeta(MAX_LATENCY, MAX_LATENCY_LABEL, LATENCY_UNIT[0], LATENCY_UNIT[1], LATENCY_SCALE, LESS_IS_BETTER)),
        (AVG_LATENCY, MetricMeta(AVG_LATENCY, AVG_LATENCY_LABEL, LATENCY_UNIT[0], LATENCY_UNIT[1], LATENCY_SCALE, LESS_IS_BETTER)),
    ])

    @property
    def metric_meta(self):
        return self.METRIC_META

    @property
    def default_metrics(self):
        return list(self.DEFAULT_METRICS)

    def get_meta(self, metric):
        return self.METRIC_META[metric]

    def create_summary_stats(self, summary, result, time):
        stats = Statistics()
        stats.data_result = result
        stats.type = StatsType.SUMMARY
        stats.time = int(time)
        mets = self.get_external_metrics(summary)
        for k, v in mets.iteritems():
            setattr(stats, k, v)
        stats.save()
        return stats

    def create_sample_stats(self, sample_csv, result):
        label_map = {v.pprint: k for k, v in self.METRIC_META.iteritems()}
        sample_lines = sample_csv.split('\n')
        header = []
        for label in sample_lines[0].split(','):
            label = label.strip().rsplit(' ', 1)[0]
            try:
                header.append(label_map[label])
            except KeyError:
                if label == self.TIME_LABEL:
                    header.append(self.TIME)

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
        stats = {}
        for k, v in self.METRIC_META.iteritems():
            if k == self.THROUGHPUT:
                stats[k] = float(summary[v.pprint + ' (requests/second)'])
            else:
                stats[k] = float(summary['Latency Distribution'][v.pprint + ' (microseconds)'])
        return stats


class Application(models.Model, BaseModel):
    user = models.ForeignKey(User)
    name = models.CharField(max_length=64, verbose_name="application name")
    description = models.TextField(null=True, blank=True)
    dbms = models.ForeignKey(DBMSCatalog)
    hardware = models.ForeignKey(Hardware)

    project = models.ForeignKey(Project)
    creation_time = models.DateTimeField()
    last_update = models.DateTimeField()

    upload_code = models.CharField(max_length=30, unique=True)
    tuning_session = models.BooleanField()
    target_objective = models.CharField(
            max_length=64,
            choices=[(k, v.pprint) for k, v in \
                     StatsManager.METRIC_META.iteritems()],
            null=True)
    nondefault_settings = models.TextField(null=True)

    def clean(self):
        if self.tuning_session is False:
            self.target_objective = None
        else:
            if self.target_objective is None:
                raise ValidationError('If this is a tuning session then '
                                      'the target objective cannot be null')
#                 self.target_objective = StatsManager.P99_LATENCY

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

    def __unicode__(self):
        return self.name


class ExpManager(models.Manager):

    def create_name(self, config, key):
        ts = config.creation_time.strftime("%m-%d-%y")
        return (key + '@' + ts + '#' + str(config.pk))


class ExpModel(models.Model, BaseModel):
    application = models.ForeignKey(Application)
    name = models.CharField(max_length=50, verbose_name="configuration name")
    description = models.CharField(max_length=512, null=True, blank=True)
    creation_time = models.DateTimeField()
    configuration = models.TextField()

    def __unicode__(self):
        return self.name

class BenchmarkConfigManager(ExpManager):

    def create_benchmark_config(self, app, config, bench_type, desc=None):
        try:
            return BenchmarkConfig.objects.get(application=app,
                                               configuration=config)
        except BenchmarkConfig.DoesNotExist:
            dom = xml.dom.minidom.parseString(config)
            root = dom.documentElement
            isolation = (root.getElementsByTagName('isolation'))[
                0].firstChild.data
            scalefactor = (
                root.getElementsByTagName('scalefactor'))[0].firstChild.data
            terminals = (root.getElementsByTagName('terminals'))[
                0].firstChild.data
            time = (root.getElementsByTagName('time'))[
                0].firstChild.data
            rate = (root.getElementsByTagName('rate'))[
                0].firstChild.data
            skew = (root.getElementsByTagName('skew'))
            skew = - \
                1 if len(skew) == 0 else skew[
                    0].firstChild.data
            transaction_types = [
                t.firstChild.data for t in root.getElementsByTagName('name')]
            transaction_weights = [
                w.firstChild.data for w in root.getElementsByTagName('weights')]

            benchmark_config = self.create(application=app,
                                           configuration=config,
                                           benchmark_type=bench_type,
                                           description=desc,
                                           creation_time=now(),
                                           isolation=isolation,
                                           scalefactor=scalefactor,
                                           terminals=terminals,
                                           time=time,
                                           rate=rate,
                                           skew=skew,
                                           transaction_types=transaction_types,
                                           transaction_weights=transaction_weights)
            benchmark_config.name = self.create_name(benchmark_config, bench_type)
            benchmark_config.save()
            return benchmark_config

class BenchmarkConfig(ExpModel):
    objects = BenchmarkConfigManager()

    benchmark_type = models.CharField(max_length=64)
    isolation = models.CharField(max_length=64, verbose_name="isolation level")
    scalefactor = models.FloatField()
    terminals = models.IntegerField()
    time = models.IntegerField(validators=[MinValueValidator(0)])
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


class DBModel(ExpModel):
    dbms = models.ForeignKey(DBMSCatalog, verbose_name="dbms")
    orig_config_diffs = models.TextField()


class DBConfManager(ExpManager):

    def create_dbconf(self, app, config, orig_config_diffs,
                      dbms, desc=None):
        try:
            return DBConf.objects.get(application=app,
                                      configuration=config)
        except DBConf.DoesNotExist:
            conf = self.create(application=app,
                               configuration=config,
                               orig_config_diffs=orig_config_diffs,
                               dbms=dbms,
                               description=desc,
                               creation_time=now())
            conf.name = self.create_name(conf, dbms.key)
            conf.save()
            return conf


class DBConf(DBModel):
    objects = DBConfManager()


class DBMSMetricsManager(ExpManager):

    def create_dbms_metrics(self, app, config, orig_config_diffs,
                            exec_time, dbms, desc=None):
        metrics = self.create(application=app,
                              configuration=config,
                              orig_config_diffs=orig_config_diffs,
                              dbms=dbms,
                              execution_time=exec_time,
                              description=desc,
                              creation_time=now())
        metrics.name = self.create_name(metrics, dbms.key)
        metrics.save()
        return metrics


class DBMSMetrics(DBModel):
    objects = DBMSMetricsManager()

    execution_time = models.IntegerField(
        validators=[MinValueValidator(0)])


class ResultManager(models.Manager):

    def create_result(self, app, dbms, bench_config, dbms_config,
                      dbms_metrics, summary, samples, timestamp,
                      summary_stats=None, task_ids=None,
                      most_similar=None):
        return self.create(application=app,
                           dbms=dbms,
                           benchmark_config=bench_config,
                           dbms_config=dbms_config,
                           dbms_metrics=dbms_metrics,
                           summary=summary,
                           samples=samples,
                           timestamp=timestamp,
                           summary_stats=summary_stats,
                           task_ids=task_ids,
                           most_similar=most_similar,
                           creation_time=now())

class Result(models.Model, BaseModel):
    objects = ResultManager()

    application = models.ForeignKey(Application, verbose_name='application name')
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
                                    validate_comma_separated_integer_list],
                                    null=True)

    def __unicode__(self):
        return unicode(self.pk)


class WorkloadClusterManager(models.Manager):

    def create_workload_cluster(self, dbms, hardware, cluster_name=None):
        if cluster_name is None:
            cluster_name = WorkloadCluster.get_default(dbms.pk, hardware.pk)
        try:
            return WorkloadCluster.objects.get(cluster_name=cluster_name)
        except WorkloadCluster.DoesNotExist:
            return self.create(dbms=dbms,
                               hardware=hardware,
                               cluster_name=cluster_name)


class WorkloadCluster(models.Model):
    __DEFAULT_FMT = '{db}_{hw}_UNASSIGNED'.format

    objects = WorkloadClusterManager()

    dbms = models.ForeignKey(DBMSCatalog)
    hardware = models.ForeignKey(Hardware)
    cluster_name = models.CharField(max_length=128, unique=True)

    @property
    def isdefault(self):
        return self.cluster_name == self.default

    @property
    def default(self):
        return self.__DEFAULT_FMT(db=self.dbms.pk,
                                  hw=self.hardware.pk)

    @staticmethod
    def get_default(dbms_id, hw_id):
        return WorkloadCluster.__DEFAULT_FMT(db=dbms_id,
                                             hw=hw_id)

    def __unicode__(self):
        return self.cluster_name


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


class Statistics(models.Model):
    objects = StatsManager()

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
