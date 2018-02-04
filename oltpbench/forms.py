'''
Created on Jul 25, 2017

@author: dvanaken
'''

from django import forms

from .models import Application, Project

class NewResultForm(forms.Form):
    upload_code = forms.CharField(max_length=30)
    sample_data = forms.FileField()
    raw_data = forms.FileField(required=False)
    db_parameters_data = forms.FileField()
    db_metrics_data = forms.FileField()
    benchmark_conf_data = forms.FileField()
    summary_data = forms.FileField()
    cluster_name = forms.CharField(max_length=128, required=False)


class ProjectForm(forms.ModelForm):

    class Meta:
        model = Project

        fields = ['name', 'description']

        widgets = {
            'name': forms.TextInput(attrs={'required': True}),
            'description': forms.Textarea(attrs={'maxlength': 500,
                                                 'rows': 5}),
        }


class ApplicationForm(forms.ModelForm):

    gen_upload_code = forms.BooleanField(widget=forms.CheckboxInput,
                                         initial=False,
                                         required=False,
                                         label='Get new upload code')

    def __init__(self, *args, **kwargs):
        super(ApplicationForm, self).__init__(*args, **kwargs)
        self.fields['description'].required = False
        self.fields['target_objective'].required = False

    class Meta:
        model = Application

        fields = ('name', 'description', 'tuning_session', 'dbms', 'hardware', 'target_objective')

        widgets = {
            'name': forms.TextInput(attrs={'required': True}),
            'description': forms.Textarea(attrs={'maxlength': 500,
                                                 'rows': 5}),
            'tuning_session': forms.CheckboxInput(attrs={'onclick': 'show_content();'}),
        }
        labels = {
            'dbms': 'DBMS',
        }
