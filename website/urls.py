from django.conf.urls import patterns, include, url

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    url(r'^login/', 'website.views.login_view'),
    url(r'^auth/', 'website.views.auth_and_login'),
    url(r'^signup/', 'website.views.sign_up_in'),
    url(r'^$', 'website.views.secured'),
    url(r'^logout/', 'website.views.logout_view'),
    # Examples:
    # url(r'^$', 'website.views.home', name='home'),
    # url(r'^website/', include('website.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),
)
