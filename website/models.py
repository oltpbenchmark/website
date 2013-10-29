# -*- coding: utf-8 -*-
from django.db import models
from django.contrib.auth.models import User
from django.db.models import signals
from django import forms

class UserProfile(models.Model):
    user = models.OneToOneField(User, related_name='profile')
    upload_code = models.CharField(max_length=100)

    def save(self, *args, **kwargs):
        try:
            existing = UserProfile.objects.get(user=self.user)
            self.id = existing.id #force update instead of insert
        except UserProfile.DoesNotExist:
            pass
        models.Model.save(self, *args, **kwargs)

def create_user_profile(sender, instance, created, **kwargs):
    if created:
        UserProfile.objects.create(user=instance)

signals.post_save.connect(create_user_profile, sender=User)

class Result(models.Model):
    user = models.ForeignKey(User)
    data = models.CharField(max_length=1000)

class UploadFileForm(forms.Form):
    upload_code = forms.CharField(max_length=30)
    title = forms.CharField(max_length=50)
    file = forms.FileField()
