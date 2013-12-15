from django.conf.urls import patterns, include, url

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
from django.contrib.staticfiles.views import serve
from django.views.decorators.cache import never_cache
from website import settings

admin.autodiscover()

urlpatterns = patterns('',
    url(r'^signup/', 'website.views.signup_view'),
    url(r'^login/', 'website.views.login_view'),
    url(r'^auth/', 'website.views.auth_and_login'),
    url(r'^signupin/', 'website.views.sign_up_in'),
    url(r'^$', 'website.views.home'),
    url(r'^logout/', 'website.views.logout_view'),

    url(r'^new_result/', 'website.views.new_result'),
    url(r'^result/', 'website.views.result'),
    url(r'^get_result_data_file/', 'website.views.get_result_data_file'),
    url(r'^update_similar/', 'website.views.update_similar'),

    url(r'^edit_project/', 'website.views.edit_project'),
    url(r'^project/', 'website.views.project'),
    url(r'^delete_project/', 'website.views.delete_project'),
    url(r'^update_project/', 'website.views.update_project'),

    url(r'^benchmark_conf/', 'website.views.benchmark_configuration'),
    url(r'^edit_benchmark_conf/', 'website.views.edit_benchmark_conf'),
    url(r'^get_benchmark_data/', 'website.views.get_benchmark_data'),
    url(r'^get_benchmark_conf_file/', 'website.views.get_benchmark_conf_file'),
    url(r'^update_benchmark/', 'website.views.update_benchmark_conf'),

    url(r'^db_conf/', 'website.views.db_conf_view'),
    url(r'^get_data/', 'website.views.get_data'),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),
)

if settings.DEBUG:
    urlpatterns += patterns('',
        url(r'^static/(?P<path>.*)$', never_cache(serve)),
    )