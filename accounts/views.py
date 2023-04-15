from django.contrib.auth.decorators import login_required
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
User = get_user_model()
from django.contrib import auth, messages
from django.db import transaction
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from django import forms
from django.shortcuts import render, redirect, get_object_or_404

from accounts.forms import UserModelForm
from collection.models import Collection, CollectionGroup, CollectionGroupUser
from datasets.models import Dataset, DatasetUser
from datasets.static.hashes import mimetypes_plus as mthash_plus
import codecs, os, re, sys, tempfile
import pandas as pd

# @login_required
def validate_usersfile(tempfn):
  print('validate_usersfile() tempfn', tempfn)
  User = get_user_model()
  # wd=os.getcwd()+'/_scratch/'
  r_email = re.compile(r'([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+')
  def emailValid(email):
    return True if re.fullmatch(r_email, email) else False
  result = {"status":'ok', "errors": [], "add": [], 'dupe': []}
  # print('validate_usersfile()', fin, result)
  import csv
  with open(tempfn, newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',',
                        skipinitialspace=True)
    for i, row in enumerate(reader):
      try:
        print('i, row', i, row)
        # delimited with comma? return for resubmit
        if len(row) != 2:
          result['errors'].append('row #'+str(i+1)+' not delimited with comma')
          result['status'] = 'failed'
          print(result)
        else:
          # got a list w/2 elements
          # 1st term an email?
          if not emailValid(row[0]):
            result['errors'].append(
              'invalid email on row #'+str(i+1)+': '+row[0])
          # is name blank?
          if row[1] == '':
            result['errors'].append('no name for row #' + str(i+1))
          if len(result['errors']) > 0:
            result['status'] = 'failed'
          else:
            # no format errors, check if user exists
            user = User.objects.filter(email=row[0])
            if not user.exists():
              result['add'].append(row)
            else:
              result['dupe'].append(row)
      except:
        raise
  print('validate result', result)
  return result

@login_required
def addusers(request):
  if request.method == 'POST':
    action = request.POST['action'] # 'upload' or 'addem'
    print('addusers() request.POST', request.POST)
    print('addusers() request.FILES', request.FILES)
    cgid = request.POST['cgid']
    cg=get_object_or_404(CollectionGroup, id=cgid)
    user = request.user.name
    print('user in addusers()::68')
    data = request.POST
    context = {}

    # uploaded file
    file = request.FILES['file']
    mimetype = file.content_type
    tempf, tempfn = tempfile.mkstemp()
    # write it to a tmp location
    try:
      for chunk in file.chunks():
        os.write(tempf, chunk)
    except:
      raise Exception("Problem opening/writing input file")
    finally:
      os.close(tempf)

    added_count = 0

    # VALIDATION
    if action == 'upload':
      # wd = os.getcwd() + '/_scratch/'
      # tempfn = wd + 'newusers.csv'
      # tempfn = wd+'badusers.csv'
      print('addusers() tempfn', tempfn)
      result = validate_usersfile(tempfn)
      print('result in addusers()', result)
    else:
      # action == 'addem' -- create users
      to_add = request.POST['newusers']
      for u in to_add:
        new_user = User.objects.create(
          email = u[0],
          name = u[1],
          # email name reversed
          password = re.match('^(.*)@', u[0]).group(1)[::-1]
        )
        new_user.save()
      # add to CollectionGroup
      cguser = CollectionGroupUser.objects.create(
        role = 'normal',
        collectiongroup = cg,
        user=new_user
      )
      cguser.save()
      added_count +=1

      result= {'status': 'added ' + str(added_count)+' users',
               'users': [u[0] for u in to_add]}

    return JsonResponse(result, safe=False)


@login_required
@transaction.atomic
def update_profile(request):
  print('update_profile() request.method',request.method)
  context = {}
  if request.method == 'POST':
    user_form = UserModelForm(request.POST, instance=request.user)
    # profile_form = ProfileModelForm(request.POST, instance=request.user.profile)
    if user_form.is_valid():
    # if user_form.is_valid() and profile_form.is_valid():
      user_form.save()
      # profile_form.save()
      messages.success(request, ('Your profile was successfully updated!'))
      return redirect('accounts:profile')
    else:
      print()
      print('error, user_form',user_form.cleaned_data)
      # print('error, profile_form',profile_form.cleaned_data)
      messages.error(request, ('Please correct the error below.'))
  else:
    user_form = UserModelForm(instance=request.user)
    # profile_form = ProfileModelForm(instance=request.user.profile)
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
        # 'profile_form': profile_form,
      'context': context
  })

def register(request):
  if request.method == 'POST':
    if request.POST['password1'] == request.POST['password2']:
      try:
        User.objects.get(email=request.POST['email'])
        return render(request, 'accounts/register.html', {'error': 'That email is already taken'})
      except User.DoesNotExist:
        print('request.POST',request.POST)
        user = User.objects.create_user(
                  request.POST['email'],
                    password=request.POST['password1'],
                    # email=request.POST['email'],
                    affiliation=request.POST['affiliation'],
                    name=request.POST['name'],
                    role='normal',
                )
        auth.login(request, user, backend='django.contrib.auth.backends.ModelBackend')
        return redirect('home')
    else:
      return render(request, 'accounts/register.html', {'error': 'Sorry, password mismatch!'})
  else:
    return render(request, 'accounts/register.html')

def login(request):
  if request.method == 'POST':
    user = auth.authenticate(email=request.POST['email'],password=request.POST['password'])
    # user = auth.authenticate(username=request.POST['username'],password=request.POST['password'])

    if user is not None:
      auth.login(request,user, backend='django.contrib.auth.backends.ModelBackend')
      return redirect('data-datasets')
    else:
      raise forms.ValidationError("Sorry, that login was invalid. Please try again.")

  else:
    return render(request, 'accounts/login.html')

def logout(request):
  if request.method == 'POST':
    auth.logout(request)
    return redirect('home')



