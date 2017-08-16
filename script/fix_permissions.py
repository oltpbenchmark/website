from fabric.api import local

path = "/var/www/ottertune"
user = "www-data"
local("sudo chown -R {0}:{0} {1}".format(user, path))
local("sudo chmod -R ugo+rX,ug+w {}".format(path))
