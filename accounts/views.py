from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User, Group
from django.contrib import auth, messages
from django.db import transaction
from django.http import HttpResponseRedirect, JsonResponse
from django import forms
from django.shortcuts import render, redirect, get_object_or_404

from accounts.forms import UserModelForm, ProfileModelForm, LoginForm
from datasets.models import Dataset, DatasetUser
from datasets.views import emailer

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
    owned = [[ds.id, ds.title, 'owner'] for ds in Dataset.objects.filter(owner = u).order_by('title')]
    collabs = [[dc.dataset_id.id, dc.dataset_id.title, dc.role] for dc in DatasetUser.objects.filter(user_id_id = id_)]
    # groups = u.groups.values_list('name', flat=True)
    groups = u.groups.all()
    group_leader = 'group_leaders' in  u.groups.values_list('name', flat=True) # True or False
    #owned.extend(collabs)
    context['owned'] = owned
    context['collabs'] = collabs
    context['comments'] = 'get comments associated with projects I own'
    context['groups'] = groups
    context['group_leader'] = group_leader

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
        user = User.objects.create_user(
                  request.POST['username'], 
                    password=request.POST['password1'],
                    email=request.POST['email'],
                    first_name=request.POST['first_name'],
                    last_name=request.POST['last_name']
                )
        user.profile.affiliation=request.POST['affiliation']
        user.profile.name=request.POST['name']
        auth.login(request, user)
        # subj, msg, from_addr, to_addr
        emailer('New WHG user', '{} ({}, id {}) just registered on the site'.format(user.username, user.profile.name, user.id),
                settings.DEFAULT_FROM_EMAIL, settings.EMAIL_TO_ADMINS)
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
      return redirect('data-datasets')
    else:
      raise forms.ValidationError("Sorry, that login was invalid. Please try again.")
      #return redirect('home', {'error': 'username or password is incorrect :^('})
  else:
    return render(request, 'accounts/login.html')

def logout(request):
  if request.method == 'POST':
    auth.logout(request)
    return redirect('home')

def create_group(request, *args, **kwargs):
  # must be member of group_leaders
  result = {"status": "", "id": "", 'name': ""}
  if request.method == 'POST':
    group_name = request.POST['group_name']
    if group_name in Group.objects.all().values_list('name', flat=True):
      result['status'] = "dupe"
    else:
      newgroup = Group.objects.create(
        name = group_name
      )
      newgroup.user_set.add(request.user)
      result = {"status": "ok", "id": newgroup.id, 'name': newgroup.name}

  return JsonResponse(result, safe=False)

# def login_view(request):
#   form = LoginForm(request.POST or None)
#   if request.POST and form.is_valid():
#     user = form.login(request)
#     if user:
#       login(request, user)
#       return HttpResponseRedirect("/")# Redirect to a success page.
#   return render(request, 'accounts/login.html', {'login_form': form })


