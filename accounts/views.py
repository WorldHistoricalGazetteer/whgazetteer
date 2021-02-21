from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User, Group
from django.contrib import auth, messages
from django.db import transaction
from django.http import HttpResponseRedirect
from django import forms
from django.shortcuts import render, redirect, get_object_or_404

from accounts.forms import UserModelForm, ProfileModelForm, LoginForm

@login_required
@transaction.atomic
def update_profile(request):
  print('update_profile() request.method',request.method)
  context = {}
  if request.method == 'POST':
    user_form = UserModelForm(request.POST, instance=request.user)
    profile_form = ProfileModelForm(request.POST, instance=request.user.profile)
    if user_form.is_valid() and profile_form.is_valid():
      user_form.save()
      profile_form.save()
      messages.success(request, ('Your profile was successfully updated!'))
      return redirect('account_profile')
    else:
      print()
      print('error, user_form',user_form.cleaned_data)
      print('error, profile_form',profile_form.cleaned_data)
      messages.error(request, ('Please correct the error below.'))
  else:
    user_form = UserModelForm(instance=request.user)
    profile_form = ProfileModelForm(instance=request.user.profile)
    id_ = request.user.id
    u = get_object_or_404(User, id=id_)
    context['projects'] = 'datasets I own or collaborate on'
    context['comments'] = 'get comments associated with projects I own'
    context['groups'] = u.groups.values_list('name',flat=True)

  return render(request, 'accounts/profile.html', {
      'user_form': user_form,
        'profile_form': profile_form,
        'context': context
    })

def register(request):
  if request.method == 'POST':
    if request.POST['password1'] == request.POST['password2']:
      try:
        User.objects.get(username=request.POST['username'])
        return render(request, 'accounts/register.html', {'error': 'User name is already taken'})
      except User.DoesNotExist:
        #print('request.POST',request.POST)
        user = User.objects.create_user(
                  request.POST['username'], 
                    password=request.POST['password1'],
                    email=request.POST['email'],
                    first_name=request.POST['first_name'],
                    last_name=request.POST['last_name']
                )
        user.profile.affiliation=request.POST['affiliation']
        #user.profile.user_type=request.POST['user_type']
        user.profile.name=request.POST['name']
        auth.login(request, user)
        return redirect('home')
    else:
      return render(request, 'accounts/register.html', {'error': 'Sorry, password mismatch!'})
  else:
    return render(request, 'accounts/register.html')

def login(request):
  if request.method == 'POST':
    user = auth.authenticate(username=request.POST['username'],password=request.POST['password'])

    if user is not None:
      auth.login(request,user)
      return redirect('dashboard')
    else:
      raise forms.ValidationError("Sorry, that login was invalid. Please try again.")
      #return redirect('home', {'error': 'username or password is incorrect :^('})
  else:
    return render(request, 'accounts/login.html')

def login_view(request):
  form = LoginForm(request.POST or None)
  if request.POST and form.is_valid():
    user = form.login(request)
    if user:
      login(request, user)
      return HttpResponseRedirect("/")# Redirect to a success page.
  return render(request, 'accounts/login.html', {'login_form': form })

def logout(request):
  if request.method == 'POST':
    auth.logout(request)
    return redirect('home')

