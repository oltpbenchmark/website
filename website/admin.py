from django.contrib import admin
from website.models import Project, Environment, Result, ExperimentConf


class ProjectAdmin(admin.ModelAdmin):
    fields = ['user', 'environment', 'name', 'description', 'creation_time',
              'last_update', 'upload_code']
    list_display = ('user', 'name', 'last_update', 'creation_time', 'environment')
    list_display_links = ('name', 'last_update', 'creation_time')

admin.site.register(Project, ProjectAdmin)


class EnvironmentAdmin(admin.ModelAdmin):
    fields = ['user', 'name', 'description', 'creation_time']
    list_display = ('user', 'name', 'creation_time')
    list_display_links = ('name', 'creation_time')

admin.site.register(Environment, EnvironmentAdmin)

admin.site.register(Result)
admin.site.register(ExperimentConf)
