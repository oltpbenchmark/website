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
    url(r'^$', website_views.home),
    url(r'^logout/', website_views.logout_view, name='logout'),

    url(r'^ajax_new/', website_views.ajax_new),
    url(r'^status/', website_views.ml_info),

    url(r'^new_result/', website_views.new_result),
    url(r'^result/', website_views.result),
    url(r'^get_result_data_file/', website_views.get_result_data_file),
    url(r'^update_similar/', website_views.update_similar),

    url(r'^edit_application/', website_views.update_application),
    url(r'^update_application/', website_views.update_application),
    url(r'^application/', website_views.application),
    url(r'^project_info/', website_views.project_info),
    url(r'^delete_application/', website_views.delete_application),
    url(r'^edit_project/', website_views.update_project),
    url(r'^project/', website_views.project),
    url(r'^delete_project/', website_views.delete_project),
    url(r'^update_project/', website_views.update_project),

    url(r'^benchmark_conf/', website_views.benchmark_configuration),
    url(r'^edit_benchmark_conf/', website_views.edit_benchmark_conf),
    url(r'^get_benchmark_data/', website_views.get_benchmark_data),
    url(r'^get_benchmark_conf_file/', website_views.get_benchmark_conf_file),
    url(r'^update_benchmark/', website_views.update_benchmark_conf),

    url(r'^db_conf/', website_views.db_conf_view),
    url(r'^dbms_metrics/', website_views.dbms_metrics_view),
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
