from django.contrib import admin
from website.models import LEARNING_PARAMS, FEATURED_PARAMS, Project, Result, ExperimentConf, DBConf,Statistics

class ExperimentConfAdmin(admin.ModelAdmin):
    list_display = [ 'name', 'application', 'benchmark_type', 'creation_time' ]
    list_filter = [ 'creation_time' ]

class ProjectAdmin(admin.ModelAdmin):
    fields = ['user', 'name', 'description', 'creation_time',
              'last_update', 'upload_code']
    list_display = ('user', 'name', 'last_update', 'creation_time')
    list_display_links = ('name', 'last_update', 'creation_time')

class DBConfAdmin(admin.ModelAdmin):
    list_display = [ 'name', 'application', 'creation_time', 'db_type' ]

class FEATURED_PARAMSAdmin(admin.ModelAdmin):
    list_display = [ 'db_type', 'params']

class LEARNING_PARAMSAdmin(admin.ModelAdmin):  
    list_display = [ 'db_type', 'params']


admin.site.register(Project, ProjectAdmin)
admin.site.register(ExperimentConf, ExperimentConfAdmin)
admin.site.register(DBConf, DBConfAdmin)
admin.site.register(FEATURED_PARAMS,FEATURED_PARAMSAdmin)
admin.site.register(LEARNING_PARAMS, LEARNING_PARAMSAdmin)


admin.site.register(Result)
