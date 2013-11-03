from django.conf.urls import patterns, include, url

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    url(r'^login/', 'website.views.login_view'),
    url(r'^auth/', 'website.views.auth_and_login'),
    url(r'^signup/', 'website.views.sign_up_in'),
    url(r'^$', 'website.views.home'),
    url(r'^logout/', 'website.views.logout_view'),
    url(r'^get_new_upload_code/', 'website.views.get_new_upload_code'),

    url(r'^new_result/', 'website.views.new_result'),
    url(r'^result/', 'website.views.result'),
    url(r'^get_result_data/', 'website.views.get_result_data'),

    url(r'^project/', 'website.views.project'),
    url(r'^new_project/', 'website.views.new_project'),
    url(r'^delete_project/', 'website.views.delete_project'),
    url(r'^update_project/', 'website.views.update_project'),

    url(r'^environment/', 'website.views.environment'),
    url(r'^new_env/', 'website.views.new_env'),
    url(r'^delete_env/', 'website.views.delete_env'),
    url(r'^update_env/', 'website.views.update_env'),

    url(r'^benchmark_conf/', 'website.views.benchmark_configuration'),
    url(r'^target_conf/', 'website.views.target_configuration'),
    # Examples:
    # url(r'^$', 'website.views.home', name='home'),
    # url(r'^website/', include('website.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),
)
