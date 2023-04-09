from django import forms
from django.contrib.auth import authenticate
from django.contrib.auth import get_user_model
User = get_user_model()

# from accounts.models import Profile

class LoginForm(forms.Form):
    email = forms.CharField(max_length=255, required=True)
    password = forms.CharField(widget=forms.PasswordInput, required=True)

    def clean(self):
        name = self.cleaned_data.get('name')
        email = self.cleaned_data.get('email')
        password = self.cleaned_data.get('password')
        user = authenticate(email=email, password=password)
        if not user or not user.is_active:
            raise forms.ValidationError("Sorry, that login was invalid. Please try again.")
        return self.cleaned_data

    def login(self, request):
        name = self.cleaned_data.get('name')
        email = self.cleaned_data.get('email')
        password = self.cleaned_data.get('password')
        user = authenticate(email=email, password=password)
        return user

# used to edit
class UserModelForm(forms.ModelForm):
    
    class Meta:
        model = User
        fields = ('email', 'name', 'affiliation', 'role')
        exclude = ('password',)

        widgets = {
            'email': forms.TextInput(attrs={'size': 30}),
            'name': forms.TextInput(attrs={'size': 30}),
            'affiliation': forms.TextInput(attrs={'size': 30}),
        }

# not in use
# class ProfileModelForm(forms.ModelForm):
#
#     class Meta:
#         model = User
#         #fields = ('name','affiliation','web_page','user_type')
#         #fields = ('affiliation','web_page','user_type')
#         fields = ('name', 'affiliation', )
#         widgets = {
#             'name': forms.TextInput(attrs={'size': 40}),
#             'affiliation': forms.TextInput(attrs={'size': 40}),
#             # 'web_page': forms.TextInput(attrs={'size': 60}),
#             'password': forms.PasswordInput(attrs={'size': 40}),
#         }
