# -*- coding: utf-8 -*-
from django.db import models
from django.contrib.auth.models import User
from django.db.models import signals
from django import forms


class UserProfile(models.Model):
    user = models.OneToOneField(User, related_name='profile')
    #upload_code = models.CharField(max_length=100)

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


class NewResultForm(forms.Form):
    upload_code = forms.CharField(max_length=30)
    data = forms.FileField()


class Environment(models.Model):
    user = models.ForeignKey(User)
    name = models.CharField(max_length=50)
    description = models.CharField(max_length=500)
    creation_time = models.DateTimeField()

    def delete(self, using=None):
        results = Result.objects.filter(environment=self)
        for r in results:
            r.delete()
        super(Environment, self).delete(using)


class Project(models.Model):
    user = models.ForeignKey(User)
    environment = models.ForeignKey(Environment)
    name = models.CharField(max_length=50)
    description = models.CharField(max_length=500)
    creation_time = models.DateTimeField()
    last_update = models.DateTimeField()

    upload_code = models.CharField(max_length=30)
    fallback_target_name = models.CharField(max_length=50)
    fallback_bench_name = models.CharField(max_length=50)

    def delete(self, using=None):
        targets = Target.objects.filter(project=self)
        results = Result.objects.filter(project=self)
        for t in targets:
            t.delete()
        for r in results:
            r.delete()
        super(Project, self).delete(using)


class Benchmark(models.Model):
    user = models.ForeignKey(User)
    name = models.CharField(max_length=50)
    description = models.CharField(max_length=500)
    configuration = models.TextField()


class Target(models.Model):
    project = models.ForeignKey(Project)
    name = models.CharField(max_length=50)
    configuration = models.TextField()


class Result(models.Model):
    project = models.ForeignKey(Project)
    environment = models.ForeignKey(Environment)
    benchmark = models.ForeignKey(Benchmark)
    target = models.ForeignKey(Target)
    timestamp = models.DateTimeField()


class Statistics(models.Model):
    result = models.ForeignKey(Result)
    time = models.IntegerField()
    throughput = models.FloatField()
    avg_latency = models.FloatField()
    min_latency = models.FloatField()
    p25_latency = models.FloatField()
    p50_latency = models.FloatField()
    p75_latency = models.FloatField()
    p90_latency = models.FloatField()
    p95_latency = models.FloatField()
    p99_latency = models.FloatField()
    max_latency = models.FloatField()

    PLOTTABLE_FIELDS = ['throughput', 'avg_latency', 'min_latency', 'p25_latency',
                        'p50_latency', 'p75_latency', 'p90_latency', 'p95_latency',
                        'p99_latency', 'max_latency']
