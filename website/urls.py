from django.conf.urls import include, url
from django.contrib import admin
from django.contrib.staticfiles.views import serve
from django.views.decorators.cache import never_cache

from website import settings
from website import views as website_views

admin.autodiscover()

urlpatterns = [
    url(r'^signup/', website_views.signup_view, name='signup'),
    url(r'^login/', website_views.login_view, name='login'),
    url(r'^logout/$', website_views.logout_view, name='logout'),

    url(r'^ajax_new/', website_views.ajax_new),
#     url(r'^status/', website_views.ml_info),

    url(r'^new_result/', website_views.new_result),
#     url(r'^result/', website_views.result),
    url(r'^get_result_data_file/', website_views.get_result_data_file),
    url(r'^update_similar/', website_views.update_similar),

    url(r'^$', website_views.redirect_home),
    url(r'^projects/$', website_views.home, name='home'),

    url(r'^projects/(?P<project_id>[0-9]+)/edit/$', website_views.update_project, name='edit_project'),
    url(r'^projects/new/$', website_views.update_project, name='new_project'),
    url(r'^projects/(?P<project_id>[0-9]+)/apps$', website_views.project, name='project'),
    url(r'^projects/delete/$', website_views.delete_project, name="delete_project"),

#     url(r'^edit_application/', website_views.update_application),
#     url(r'^edit_application/(?P<project_id>[0-9]+)/$', website_views.update_application, name='edit_application'),
#     url(r'^edit_application/(?P<app_id>[0-9]+)/$', website_views.update_application, name='edit_application'),
#     url(r'^edit_application/(?P<project_id>[0-9]+)/(?P<app_id>[0-9]+)/$', website_views.update_application, name='edit_application'),
#     url(r'^update_application/', website_views.update_application, name='update_application'),
#     url(r'^application/', website_views.application, 'application'),
    url(r'^projects/(?P<project_id>[0-9]+)/apps/(?P<app_id>[0-9]+)/$', website_views.application, name='application'),
#     url(r'^project_info/', website_views.project_info),
    url(r'^projects/(?P<project_id>[0-9]+)/apps/delete/$', website_views.delete_application, name='delete_application'),
    url(r'^projects/(?P<project_id>[0-9]+)/apps/new/$', website_views.update_application, name='new_application'),
    url(r'^projects/(?P<project_id>[0-9]+)/apps/(?P<app_id>[0-9]+)/edit/$', website_views.update_application, name='edit_application'),
    
    url(r'^projects/(?P<project_id>[0-9]+)/apps/(?P<app_id>[0-9]+)/results/(?P<result_id>[0-9]+)/$', website_views.result, name='result'),
    url(r'^projects/(?P<project_id>[0-9]+)/apps/(?P<app_id>[0-9]+)/bench_confs/(?P<bench_id>[0-9]+)/$', website_views.benchmark_configuration, name='bench_conf'),
    url(r'^projects/(?P<project_id>[0-9]+)/apps/(?P<app_id>[0-9]+)/bench_confs/(?P<bench_id>[0-9]+)/edit/$', website_views.edit_benchmark_conf, name='edit_bench_conf'),
#     url(r'^projects/(?P<project_id>[0-9]+)/apps/(?P<app_id>[0-9]+)/db_confs/(?P<dbconf_id>[0-9]+)/compare=(?P<compare>[0-9]+)/$', website_views.db_conf_view, name='db_confs_compare'),
    url(r'^projects/(?P<project_id>[0-9]+)/apps/(?P<app_id>[0-9]+)/db_confs/(?P<dbconf_id>[0-9]+)/$', website_views.db_conf_view, name='db_confs'),
    url(r'^projects/(?P<project_id>[0-9]+)/apps/(?P<app_id>[0-9]+)/db_metrics/(?P<dbmet_id>[0-9]+)/$', website_views.db_metrics_view, name='db_metrics'),
#     url(r'^projects/(?P<project_id>[0-9]+)/apps/(?P<app_id>[0-9]+)/db_metrics/(?P<dbmet_id>[0-9]+)/\?compare=(?P<compare>[0-9]+)/$', website_views.db_metrics_view, name='db_metrics'),
    url(r'^ref/(?P<dbms_name>.+)/(?P<version>.+)/params/(?P<param_name>.+)/$', website_views.db_conf_ref, name="dbconf_ref"),
    url(r'^ref/(?P<dbms_name>.+)/(?P<version>.+)/metrics/(?P<metric_name>.+)/$', website_views.db_metrics_ref, name="dbmetrics_ref"),
    url(r'^projects/(?P<project_id>[0-9]+)/apps/(?P<app_id>[0-9]+)/results/(?P<result_id>[0-9]+)/status$', website_views.ml_info, name="status"),

#     url(r'^project/', website_views.project),
#     url(r'^update_project/', website_views.update_project, name='update_project'),

#     url(r'^benchmark_conf/', website_views.benchmark_configuration),
#     url(r'^edit_benchmark_conf/', website_views.edit_benchmark_conf),
    url(r'^get_benchmark_data/', website_views.get_benchmark_data),
    url(r'^get_benchmark_conf_file/', website_views.get_benchmark_conf_file),
#     url(r'^update_benchmark/', website_views.update_benchmark_conf),

#     url(r'^db_conf/', website_views.db_conf_view),
#     url(r'^db_conf_ref/', website_views.db_conf_ref),
#     url(r'^dbms_metrics/', website_views.dbms_metrics_view),
#     url(r'^dbms_metrics_ref/', website_views.dbms_metrics_ref),
    url(r'^get_data/', website_views.get_timeline_data),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    url(r'^admin/', admin.site.urls),
]

if settings.DEBUG:
    import debug_toolbar

    urlpatterns = [
        url(r'^__debug__/', include(debug_toolbar.urls)),
    ] + urlpatterns + [url(r'^static/(?P<path>.*)$', never_cache(serve)), ]
