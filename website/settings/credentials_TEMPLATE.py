"""
Private/custom Django settings for the OtterTune project.

"""

## ==============================================
## SECRET KEY CONFIGURATION
## ==============================================

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'ADD ME!!'

## ==============================================
## DATABASE CONFIGURATION
## ==============================================

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'ottertune', 
        'USER': 'ADD ME!!',
        'PASSWORD': 'ADD ME!!',
        'HOST': '',
        'PORT': '',
        'OPTIONS': {
            'init_command': "SET sql_mode='STRICT_TRANS_TABLES',innodb_strict_mode=1",
        },
    }
}
