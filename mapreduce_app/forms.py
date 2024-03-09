from django import forms

class ConfigForm(forms.Form):
    APPLICATION_CHOICES = [
        ('word_count', 'Word Count'),
        ('inverted_index', 'Inverted Index'),
    ]
    application = forms.ChoiceField(choices=APPLICATION_CHOICES, label="Application")
    mapper_count = forms.IntegerField(label='Mapper Count', min_value=1)
    reducer_count = forms.IntegerField(label='Reducer Count', min_value=1)

class TextInputForm(forms.Form):
    text = forms.CharField(widget=forms.Textarea, required=False)
    file = forms.FileField(required=False)