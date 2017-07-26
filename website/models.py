from collections import OrderedDict

from django.db import models
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.core.validators import validate_comma_separated_integer_list

from .types import DBMSType, MetricType, VarType, HardwareType, TaskType, KnobUnitType, PipelineComponentType
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
        if self.metric_type == MetricType.COUNTER and self.vartype != VarType.INTEGER:
            raise ValidationError('Counter metrics must be integers.')


class Project(models.Model):
    user = models.ForeignKey(User)
    name = models.CharField(max_length=64)
    description = models.TextField(null=True)
    creation_time = models.DateTimeField()
    last_update = models.DateTimeField()

    upload_code = models.CharField(max_length=30)

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
    storage = models.CharField(max_length=64,
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
  
    def delete(self, using=None):
        targets = DBConf.objects.filter(application=self)
        results = Result.objects.filter(application=self)
        expconfs =  BenchmarkConfig.objects.filter(application=self)
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
    transaction_types = models.TextField(validators=[validate_comma_separated_integer_list])
    transaction_weights = models.TextField(validators=[validate_comma_separated_integer_list])

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
        if return_both == True:
            other_params = OrderedDict()
        for p in param_catalog:
            if p.tunable == True:
                tunable_params[p.name] = config[p.name]
            elif return_both == True:
                other_params[p.name] = config[p.name]
        return tunable_params if return_both == False else (tunable_params, other_params)


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
        if return_both == True:
            other_metrics = OrderedDict()
        for m in metric_catalog:
            if m.metric_type == MetricType.COUNTER:
                numeric_metrics[m.name] = float(config[m.name]) / self.execution_time \
                        if normalize == True else float(config[m.name])
            elif return_both == True:
                other_metrics[m.name] = config[m.name]
        return numeric_metrics if return_both == False else (numeric_metrics, other_metrics)

class Result(models.Model):
    application = models.ForeignKey(Application)
    dbms = models.ForeignKey(DBMSCatalog)
    benchmark_config = models.ForeignKey(BenchmarkConfig)
    dbms_config = models.ForeignKey(DBConf)
    dbms_metrics = models.ForeignKey(DBMSMetrics)

    creation_time = models.DateTimeField()
    summary = models.TextField()
    samples = models.TextField()
    task_ids = models.CharField(max_length=64, validators=[validate_comma_separated_integer_list], null=True)

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
    most_similar = models.CharField(max_length=100, validators=[validate_comma_separated_integer_list])

    def __unicode__(self):
        return unicode(self.pk)


class ResultData(models.Model):
    dbms = models.ForeignKey(DBMSCatalog)
    hardware = models.ForeignKey(Hardware)
    result = models.ForeignKey(Result)
    param_data = models.TextField()
    metric_data = models.TextField()


class Task(models.Model):
    taskmeta_id = models.CharField(max_length=255, unique=True)
    start_time = models.DateTimeField(null=True)
    result = models.ForeignKey(Result)
    type = models.IntegerField(choices=TaskType.choices())


class PipelineResult(models.Model):
    dbms = models.ForeignKey(DBMSCatalog)
    hardware = models.ForeignKey(Hardware)
    version_id = models.IntegerField()
    component = models.IntegerField(choices=PipelineComponentType.choices())
    task_type = models.IntegerField()
    value = models.TextField()

    def clean_fields(self, exclude=None):
        super(PipelineResult, self).clean_fields(exclude=exclude)
        if self.task_type not in PipelineComponentType.TASK_TYPES[self.component].choices():
            raise ValidationError("Invalid task type for component {} ({})".format(self.get_component_display(),
                                                                                   self.task_type))

    class Meta:
        unique_together = ("dbms", "hardware", "version_id", "component", "task_type")

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
    'p99_latency': {'unit': 'milliseconds', 'lessisbetter': True, 'scale': 0.001, 'print': '99% Latency'},
    'p95_latency': {'unit': 'milliseconds', 'lessisbetter': True, 'scale': 0.001, 'print': '95% Latency'},
    'p90_latency': {'unit': 'milliseconds', 'lessisbetter': True, 'scale': 0.001, 'print': '90% Latency'},
    'p75_latency': {'unit': 'milliseconds', 'lessisbetter': True, 'scale': 0.001, 'print': '75% Latency'},
    'p50_latency': {'unit': 'milliseconds', 'lessisbetter': True, 'scale': 0.001, 'print': 'Med. Latency'},
    'p25_latency': {'unit': 'milliseconds', 'lessisbetter': True, 'scale': 0.001, 'print': '25% Latency'},
    'min_latency': {'unit': 'milliseconds', 'lessisbetter': True, 'scale': 0.001, 'print': 'Min Latency'},
    'avg_latency': {'unit': 'milliseconds', 'lessisbetter': True, 'scale': 0.001, 'print': 'Avg. Latency'},
    'max_latency': {'unit': 'milliseconds', 'lessisbetter': True, 'scale': 0.001, 'print': 'Max Latency'}
}

