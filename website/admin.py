from django.contrib import admin
from website.models import Project, Result, ExperimentConf, DBConf


class ExperimentConfAdmin(admin.ModelAdmin):
    list_display = [ 'name', 'project', 'benchmark_type', 'creation_time' ]
    list_filter = [ 'creation_time' ]

class ProjectAdmin(admin.ModelAdmin):
    fields = ['user', 'name', 'description', 'creation_time',
              'last_update', 'upload_code']
    list_display = ('user', 'name', 'last_update', 'creation_time')
    list_display_links = ('name', 'last_update', 'creation_time')

class DBConfAdmin(admin.ModelAdmin):
    list_display = [ 'name', 'project', 'creation_time', 'db_type' ]

admin.site.register(Project, ProjectAdmin)
admin.site.register(ExperimentConf, ExperimentConfAdmin)
admin.site.register(DBConf, DBConfAdmin)

admin.site.register(Result)
