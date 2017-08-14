'''
Created on Jul 25, 2017

@author: dvanaken
'''

from django import forms
from django.utils.translation import ugettext_lazy as _

from .models import Application, DBMSCatalog, Hardware
from .types import DBMSType, HardwareType

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


class ApplicationForm(forms.ModelForm):

    dbms = forms.ModelChoiceField(queryset=DBMSCatalog.objects.all(),
                                  initial=DBMSCatalog.objects.get(type=DBMSType.POSTGRES,
                                                                  version='9.6'),
                                  label='DBMS')

    hardware = forms.ModelChoiceField(queryset=Hardware.objects.all(),
                                      initial=Hardware.objects.get(type=HardwareType.EC2_M3XLARGE),
                                      label='Hardware')

    gen_upload_code = forms.BooleanField(widget=forms.CheckboxInput,
                                         initial=False,
                                         required=False,
                                         label='Get new upload code')

    class Meta:
        model = Application

        fields = ('name', 'description', 'tuning_session', 'dbms', 'hardware')

        widgets = {
            'name': forms.TextInput(attrs={'required': True}),
            'description': forms.Textarea(attrs={'title': 'Description',
                                                 'required': False,
                                                 'maxlength': 500,
                                                 'rows': 5}),
            'tuning_session': forms.CheckboxInput(),
        }
        labels = {
            'name': _('Application name'),
            'description': _('Description'),
            'tuning_session': _('Tuning session'),
        }
