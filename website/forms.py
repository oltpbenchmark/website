'''
Created on Jul 25, 2017

@author: dvanaken
'''

from django import forms


class NewResultForm(forms.Form):
    upload_code = forms.CharField(max_length=30)
    sample_data = forms.FileField()
    raw_data = forms.FileField(required=False)
    db_parameters_data = forms.FileField()
    db_metrics_data = forms.FileField()
    benchmark_conf_data = forms.FileField()
    summary_data = forms.FileField()
    cluster_name = forms.CharField(max_length=128, required=False)


class TuningSessionCheckbox(forms.Form):
    tuning_session = forms.BooleanField(
        required=False, label="Tuning Session:", widget=forms.CheckboxInput())
