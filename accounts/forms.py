from django import forms
from django.contrib.auth import authenticate
from django.contrib.auth import get_user_model
User = get_user_model()

# from accounts.models import Profile

class LoginForm(forms.Form):
    email = forms.CharField(max_length=255, required=True)
    password = forms.CharField(widget=forms.PasswordInput, required=True)

    def clean(self):
        username = self.cleaned_data.get('username')
        password = self.cleaned_data.get('password')
        user = authenticate(username=username, password=password)
        if not user or not user.is_active:
            raise forms.ValidationError("Sorry, that login was invalid. Please try again.")
        return self.cleaned_data

    def login(self, request):
        username = self.cleaned_data.get('username')
        password = self.cleaned_data.get('password')
        user = authenticate(username=username, password=password)
        return user

# not currently used
class UserModelForm(forms.ModelForm):
    
    class Meta:
        model = User
        fields = ('username', 'email', 'first_name', 'last_name',)
        exclude = ('password',)

# in use
class ProfileModelForm(forms.ModelForm):
    
    class Meta:
        model = User
        #fields = ('name','affiliation','web_page','user_type')
        #fields = ('affiliation','web_page','user_type')
        fields = ('name', 'affiliation', )
        widgets = {
            'name': forms.TextInput(attrs={'size': 40}),
            'affiliation': forms.TextInput(attrs={'size': 40}),
            # 'web_page': forms.TextInput(attrs={'size': 60}),
            'password': forms.PasswordInput(attrs={'size': 40}),
        }
