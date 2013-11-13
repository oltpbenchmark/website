from django.contrib import admin
from website.models import Project, Result, ExperimentConf


class ProjectAdmin(admin.ModelAdmin):
    fields = ['user', 'name', 'description', 'creation_time',
              'last_update', 'upload_code']
    list_display = ('user', 'name', 'last_update', 'creation_time')
    list_display_links = ('name', 'last_update', 'creation_time')

admin.site.register(Project, ProjectAdmin)


admin.site.register(Result)
admin.site.register(ExperimentConf)
