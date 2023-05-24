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
from collection.models import Collection, CollectionGroup, CollectionGroupUser, CollectionUser
from datasets.models import Dataset, DatasetUser
from datasets.static.hashes import mimetypes_plus as mthash_plus
import codecs, json, os, re, sys, tempfile
import pandas as pd

# @login_required
# validate CollectionGroup member file upload
def validate_usersfile(tempfn, cg):
  print('validate_usersfile() tempfn', tempfn)
  User = get_user_model()
  # wd=os.getcwd()+'/_scratch/'
  r_email = re.compile(r'([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+')
  def emailValid(email):
    return True if re.fullmatch(r_email, email) else False
  # buckets
  result = {"status":'validated', "errors": [], "create_add": [],
            'just_add': [], 'already': []}
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
          # 1st term valid email?
          if not emailValid(row[0]):
            result['errors'].append(
              'invalid email on row #'+str(i+1)+': '+row[0])
          # is name blank?
          if row[1] == '':
            result['errors'].append('no name for row #' + str(i+1))
          # if errors, return them
          if len(result['errors']) > 0:
            result['status'] = 'failed'
          else:
            # no format errors
            # user exists? in this group?
            user = User.objects.filter(email=row[0])
            members = [u.user_id for u in cg.members.all()]
            if user.exists():
              in_group = user[0].id in members
              if in_group:
                result['already'].append(row)
              else:
                result['just_add'].append(row)
            else:
              result['create_add'].append(row)
      except:
        raise
  # print('validate result', result)
  return result

def add_to_group(cg,member):
  print('add_to_group', cg, member)
  cguser = CollectionGroupUser.objects.create(
    role='normal',
    collectiongroup=cg,
    user=member
  )
  cguser.save()

@login_required
def addusers(request):
  if request.method == 'POST':
    action = request.POST['action'] # 'upload' or 'addem'
    print('addusers() request.POST', request.POST)
    cgid = request.POST['cgid']
    cg=get_object_or_404(CollectionGroup, id=cgid)
    created_count = 0
    only_joined_count = 0
    new_members=[]

    # VALIDATION
    if action == 'upload':
      print('in addusers(), upload')
      print('addusers() request.FILES', request.FILES)
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

      print('addusers() tempfn', tempfn)
      # validate file and return results
      result = validate_usersfile(tempfn, cg)
      print('validation result', result)
    elif action == 'addem':
      try:
        # process 'create_add' and 'just_add'
        create_add = json.loads(request.POST['create_add']) or None
        just_add = json.loads(request.POST['just_add']) or None
        print('just_add',just_add)
        print('create_add',create_add)
        # return
        # create new
        if create_add:
          for u in create_add:
            print('u', u)
            new_user = User.objects.create(
              email = u[0],
              name = u[1],
              # email name reversed
              password = re.match('^(.*)@', u[0]).group(1)[::-1]
            )
            new_user.save()
            add_to_group(cg,new_user)
            created_count +=1
            new_members.append([u[0], u[1], new_user.id])
        # add all to group
        if just_add:
          for u in just_add:
            user=User.objects.get(email=u[0])
            print('user', user)
            add_to_group(cg, user)
            new_members.append([u[0], u[1], user.id])
            # cguser = CollectionGroupUser.objects.create(
            #   role='normal',
            #   collectiongroup=cg,
            #   user=user
            # )
            # cguser.save()
            only_joined_count +=1
        total = created_count+only_joined_count
        result = {
          'status': 'added', 'errors': [],
          'newmembers': new_members,
          'msg': '<p>Created ' + str(created_count) + ' new WHG users</p>' +
                 '<p>Added <b>'+str(total)+'</b> new group members</p>'
        }
      except:
        result = {'status': 'failed', 'errors': sys.exc_info(),
                  'msg': 'something went wrong!'}

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
    ds_owned = [[ds.id, ds.title, 'owner'] for ds in Dataset.objects.filter(owner = u).order_by('title')]
    ds_collabs = [[dc.dataset_id.id, dc.dataset_id.title, dc.role] for dc in DatasetUser.objects.filter(user_id_id = id_)]
    # groups = u.groups.values_list('name', flat=True)
    groups_owned = u.groups.all()
    group_leader = 'group_leaders' in  u.groups.values_list('name', flat=True) # True or False

    context['ds_owned'] = ds_owned
    context['ds_collabs'] = ds_collabs
    context['coll_owned'] = Collection.objects.filter(owner=u)
    context['coll_collab'] = CollectionUser.objects.filter(user = u)
    context['collections'] = Collection.objects.filter(owner=u)
    context['groups_owned'] = groups_owned
    context['mygroups'] = [ g.collectiongroup for g in CollectionGroupUser.objects.filter(user=u)]
    context['group_leader'] = group_leader
    context['comments'] = 'get comments associated with projects I own'

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



