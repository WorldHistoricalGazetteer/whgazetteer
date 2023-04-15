import os

from django.test import TestCase
from django.conf import settings
# Create your tests here.
from collection.models import *
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
User = get_user_model()
# get a normal user
user = User.objects.get(id=12) #another
# get a user with role == group_leader
leader = User.objects.get(id=6) #sage
import os, codecs, re, json


# create a place collection
pcoll = Collection.objects.create(
# owner, title, description, keywords, omitted, rel_keywords, creator, contact, webpage,
  # collection_class, image_file, file, created, status, featured, places, datasets
  owner=user, # someuser@kgeographer.org
  title='Student example {n}',
  description='just an empty one',
  collection_class='place',
  status='group',
  keywords=['am','I','preserved'],
  nominated = False
)
pcoll.save()
passed = pcoll.title == 'Student example {n}'
if passed:
  print('ok')
  # Collection.objects.filter(title='Place Collection test 01').delete()
else:
  print('whoops, failed')

# if cg: cg.delete();
cg2 = CollectionGroup.objects.create(
  # owner, group, title, description, keywords, collections
  owner=leader,
  title="Draft Collection Group",
  description='just an idea',
  keywords=['world history', 'connections', 'diaspora'],
)
cg2.save()
cg2.collections.add(pcoll)
# cg2.collections.add(*[c for c in Collection.objects.all()])
print(cg2.collections.all())

u1=User.objects.get(id=7)
u2=User.objects.get(id=12)
# users collaborate on each others' collection
CollectionUser.objects.create(
  collection=Collection.objects.get(id=28),
  user = u1,
  role = 'member'
).save()
CollectionUser.objects.create(
  collection=Collection.objects.get(id=1),
  user = u2,
  role='member'
).save()

# group_leader adds 2 users to CollectionGroup
CollectionGroupUser.objects.create(
  collectiongroup=cg2,
  user = u1,
  role = 'member'
).save()

CollectionGroupUser.objects.create(
  collectiongroup=cg,
  user = u2,
  role='member'
).save()

cg= CollectionGroup.objects.get(id=30)
import json, pprint
pprint.pprint({
  'cg': cg.title,
  'group leader': cg.owner.name,
  'members': [u.user.name + '('+u.user.email+')' for u in cg.members.all()],
  'collections': [(c.id, c.title, c.owner.name  ) for c in cg.collections.all()]
})

cg.delete()
pcoll.submitted=True;
pcoll.save();