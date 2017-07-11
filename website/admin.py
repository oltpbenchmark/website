from django.contrib import admin
from website.models import *

class DBMSCatalogAdmin(admin.ModelAdmin):
    list_display = ['dbms_info']
    
    def dbms_info(self, obj):
        return obj.full_name


class KnobCatalogAdmin(admin.ModelAdmin):
    list_display = ['name', 'dbms_info', 'tunable']
    ordering = ['name', 'dbms__type', 'dbms__version']
    list_filter = ['tunable']

    def dbms_info(self, obj):
        return obj.dbms.full_name


class MetricCatalogAdmin(admin.ModelAdmin):
    list_display = ['name', 'dbms_info', 'metric_type']
    ordering = ['name', 'dbms__type', 'dbms__version']
    list_filter = ['metric_type']

    def dbms_info(self, obj):
        return obj.dbms.full_name


class ProjectAdmin(admin.ModelAdmin):
    list_display = ('name', 'user', 'last_update', 'creation_time')
    fields = ['name', 'user', 'last_update', 'creation_time']


class ApplicationAdmin(admin.ModelAdmin):
    fields = ['name', 'user', 'description', 'creation_time', 'last_update', 'upload_code']
    list_display = ('name', 'user', 'last_update', 'creation_time')
    list_display_links = ('name',)


class BenchmarkConfigAdmin(admin.ModelAdmin):
    list_display = ['name', 'benchmark_type', 'creation_time']
    list_filter = [ 'benchmark_type' ]
    fields = ['application', 'name', 'benchmark_type', 'creation_time', 'isolation',
              'scalefactor', 'terminals', 'rate', 'time', 'skew', 'configuration']


class DBConfAdmin(admin.ModelAdmin):
    list_display = [ 'name', 'dbms_info', 'creation_time']
    fields = ['application', 'name', 'creation_time', 'tuning_configuration', 'configuration', 'dbms']
    
    def dbms_info(self, obj):
        return obj.dbms.full_name


class DBMSMetricsAdmin(admin.ModelAdmin):
    list_display = ['name', 'dbms_info', 'creation_time']
    fields = ['application', 'name', 'creation_time', 'execution_time', 'configuration', 'dbms']

    def dbms_info(self, obj):
        return obj.dbms.full_name


class TaskAdmin(admin.ModelAdmin):
    list_display = ['result', 'status', 'creation_time', 'finish_time']


class ResultAdmin(admin.ModelAdmin):
    list_display = ['result_id', 'dbms_info', 'benchmark', 'creation_time']
    list_filter = ['dbms__type', 'dbms__version', 'benchmark_config__benchmark_type']
    ordering = ['id']
    
    def result_id(self, obj):
        return obj.id

    def dbms_info(self, obj):
        return obj.dbms.full_name
    
    def benchmark(self, obj):
        return obj.benchmark_config.benchmark_type


admin.site.register(DBMSCatalog, DBMSCatalogAdmin)
admin.site.register(KnobCatalog, KnobCatalogAdmin)
admin.site.register(MetricCatalog, MetricCatalogAdmin)
admin.site.register(Application, ApplicationAdmin)
admin.site.register(Project, ProjectAdmin)
admin.site.register(BenchmarkConfig, BenchmarkConfigAdmin)
admin.site.register(DBConf, DBConfAdmin)
admin.site.register(DBMSMetrics, DBMSMetricsAdmin)
admin.site.register(Task, TaskAdmin)
admin.site.register(Result, ResultAdmin)
