"""
Tests for User and Group functions
added Apr 2023 @kgeographer
"""
import os

from django.contrib.auth import get_user_model
User=get_user_model()
from django.conf import settings
from django.test import TestCase
from django.urls import reverse

from datasets.models import Dataset
from collection.models import Collection, CollectionGroup
# TODO: shouldn't only be in remote app
from remote.serializers import CollectionRemoteSerializer

# from places.models import Place
from rest_framework import status
from rest_framework.test import APIClient

# list a user's datasets
DATASETS_URL = reverse('data-datasets')
def dataset_detail_url(dataset_id):
	"""Create and return a dataset detail URL."""
	return reverse('datasets:ds_summary', args=[dataset_id])

# list a user's collections
COLLECTIONS_URL = reverse('data-collections')
def collection_detail_url(collection_id):
	"""Create and return a collection detail URL."""
	return reverse('collection:place-collection-browse', args=[collection_id])

# list a user's profile
PROFILE_URL = reverse('accounts:profile')
def collectiongroup_detail_url(collectiongroup_id):
	"""Create and return a collection group detail URL."""
	return reverse('collection:collectiongroup-update', args=[collectiongroup_id])

def create_user(**params):
	"""Create and return a new user."""
	return settings.AUTH_USER_MODEL.create_user(**params)
	# return get_user_model().objects.create_user(**params)

def create_place_collection(user, **params):
	"""Create and return a place collection w/required fields"""
	defaults = {
		'title': 'Sample collection title',
		'description': 'Sample collection description',
		'collection_class': 'place'
	}
	defaults.update(params)

	collection = Collection.objects.create(owner=user, **defaults)
	return collection

def create_collection_group(user, **params):
	"""Create and return a sample collection_group w/required fields"""
	defaults = {
		'title': 'Making Connections',
		'description': 'Sample collection description'
	}
	defaults.update(params)

	collection_group = CollectionGroup.objects.create(owner=user, **defaults)
	return collection_group

"""Test unauthenticated API requests to list user objects fail"""
""" ok 9 Apr 2023 """
class PublicAPITests(TestCase):
	def setUp(self):
		self.client = APIClient()

	def test_auth_required_ds(self):
		res = self.client.get(DATASETS_URL)
		# redirects w/ a redirect
		self.assertRedirects(res, '/accounts/login/?redirect_to=/mydata/',
			status_code=302, target_status_code=200, fetch_redirect_response=True)
		# self.assertEqual(res.status_code, status.HTTP_302_FOUND)

	def test_auth_required_coll(self):
		res = self.client.get(COLLECTIONS_URL)
		# redirects w/ a redirect
		self.assertRedirects(res, '/accounts/login/?redirect_to=/mycollections/',
			status_code=302, target_status_code=200, fetch_redirect_response=True)

	# collection groups are listed in profile
	def test_auth_required_profile(self):
		res = self.client.get(PROFILE_URL)
		# redirects w/ a redirect
		self.assertRedirects(res, '/accounts/login/?next=/accounts/profile/',
			status_code=302, target_status_code=200, fetch_redirect_response=True)

class PrivateAPITests(TestCase):
	def setUp(self):
		self.client = APIClient()
		self.user = create_user(email='testy@example.com',
														password='test123', name='user1')
		self.client.force_authenticate(self.user)

	def test_collection_list_limited_to_user(self):
		"""Test list of collections is limited to authenticated user."""
		other_user = create_user(email='other@example.com',
														 password='test123', name='user3')
		create_place_collection(user=self.user)

		self.client.force_authenticate(other_user)
		create_place_collection(user=other_user)

		res = self.client.get(COLLECTIONS_URL)
		# print('res', res)
		count1 = Collection.objects.filter(owner=self.user).count()
		self.assertEqual(count1, 1)
		count2 = Collection.objects.filter(owner=other_user).count()
		self.assertEqual(count2, 1)

	def test_create_and_list_collections(self):
		"""Test creating & listing a user's collections"""
		create_place_collection(user=self.user, title='testy')
		create_place_collection(user=self.user, title='besty')

		res = self.client.get(COLLECTIONS_URL)
		collections = Collection.objects.all().order_by('-id')
		serializer = CollectionRemoteSerializer(collections, many=True)
		first=Collection.objects.first().title
		last=Collection.objects.last().title
		county = Collection.objects.filter(owner=self.user).count()
		self.assertRedirects(res, '/accounts/login/?redirect_to=/mycollections/',
			status_code=302, target_status_code=200, fetch_redirect_response=True)
		self.assertEqual(county, 2)
		self.assertEqual(first, 'testy')
		self.assertEqual(last, 'besty')

	def test_leader_create_collectiongroup(self):
		other_user = create_user(
			email='other@example.com', role='group_leader',
			password='test123', name='user3')

		create_collection_group(other_user)
		cg = CollectionGroup.objects.first().title
		print('leader, cg', other_user.name, cg)

		# non-group_leader
		create_collection_group(self.user, title='Should not happen')
		cg = CollectionGroup.objects.last().title
		print('user, cg', other_user.name, cg)



"""Test authenticated API requests."""
# class PrivateApiTests(TestCase):
# 	""" DATASETS: list, detail, create """
# 	def setUp(self):
# 		self.client = APIClient()
# 		self.user = create_user(email='user@example.com', password='test123', username='user1')
# 		self.client.force_authenticate(self.user)
#
# 	def test_retrieve_datasets(self):
# 		"""Test retrieving a list of all datasets"""
# 		create_dataset(user=self.user, label="example_1")
# 		create_dataset(user=self.user, label="example_2")
#
# 		res = self.client.get(DATASETS_URL)
#
# 		datasets = Dataset.objects.all().order_by('-id')
# 		serializer = DatasetRemoteSerializer(datasets, many=True)
# 		# print('res.data.results', res.data.get('results'))
# 		# print('serializer.data', serializer.data)
#
# 		self.assertEqual(res.status_code, status.HTTP_200_OK)
# 		self.assertEqual(res.data.get('results'), serializer.data)
#
# 	def test_dataset_list_limited_to_user(self):
# 		"""Test list of datasets is limited to authenticated user."""
# 		other_user = create_user(email='other@example.com', password='test123', username='user2')
# 		# Datasets must have unique labels
# 		create_dataset(user=other_user, label='example3')
# 		create_dataset(user=self.user, label='example4')
#
# 		res = self.client.get(DATASETS_URL)
#
# 		datasets = Dataset.objects.filter(owner=self.user)
# 		serializer = DatasetRemoteSerializer(datasets, many=True)
# 		self.assertEqual(res.status_code, status.HTTP_200_OK)
# 		self.assertEqual(res.data.get('results'), serializer.data)
#
# 	def test_get_dataset_detail(self):
# 		"""Test get dataset detail"""
# 		dataset = create_dataset(user=self.user, label="example_5" )
#
# 		url = dataset_detail_url(dataset.id)
# 		res = self.client.get(url)
#
# 		serializer = DatasetRemoteDetailSerializer(dataset)
#
# 		self.assertEqual(res.data, serializer.data)
#
# 	def test_create_dataset(self):
# 		"""Test creating a dataset."""
# 		payload = {
# 			'owner': self.user,
# 			'title': 'Sample POSTed dataset',
# 			'label': 'example_6',
# 			'description': 'Created with POST action of ViewSet'
# 		}
# 		res = self.client.post(DATASETS_URL, payload)
#
# 		self.assertEqual(res.status_code, status.HTTP_201_CREATED)
# 		dataset = Dataset.objects.get(id=res.data['id'])
# 		for k, v in payload.items():
# 			self.assertEqual(getattr(dataset, k), v)
# 		self.assertEqual(dataset.owner, self.user)
#
# 	def create_dataset(user, **params):
# 		"""Create and return a sample dataset; params incl. unique label"""
# 		defaults = {
# 			'title': 'Sample recipe title',
# 			'description': 'Sample description',
# 			'public': False,
# 			'numrows': 99
# 		}
# 		defaults.update(params)
#
# 		dataset = Dataset.objects.create(owner=user, **defaults)
# 		return dataset
#
# 	""" COLLECTIONS: list, detail, create, update (add place) """



	# def test_collection_list_limited_to_user(self):
	# 	"""Test list of collections is limited to authenticated user."""
	# 	other_user = create_user(email='other@example.com', password='test123', username='user3')
	# 	create_collection(user=other_user)
	# 	create_collection(user=self.user)
	#
	# 	res = self.client.get(COLLECTIONS_URL)
	#
	# 	collections = Collection.objects.filter(owner=self.user)
	# 	serializer = CollectionRemoteSerializer(collections, many=True)
	#
	# 	self.assertEqual(res.status_code, status.HTTP_200_OK)
	# 	self.assertEqual(res.data.get('results'), serializer.data)
	#
	# def test_retrieve_collections(self):
	# 	"""Test retrieving a list of all collections"""
	# 	create_collection(user=self.user)
	# 	create_collection(user=self.user)
	#
	# 	res = self.client.get(COLLECTIONS_URL)
	#
	# 	collections = Collection.objects.all().order_by('-id')
	# 	serializer = CollectionRemoteSerializer(collections, many=True)
	#
	# 	self.assertEqual(res.status_code, status.HTTP_200_OK)
	# 	self.assertEqual(res.data.get('results'), serializer.data)

	# def test_get_collection_detail(self):
	# 	"""Test get dataset detail"""
	# 	dataset = create_dataset(user=self.user, label="example_5" )
	#
	# 	url = dataset_detail_url(dataset.id)
	# 	res = self.client.get(url)
	# 	# print('res.data', res.data)
	#
	# 	serializer = DatasetRemoteDetailSerializer(dataset)
	# 	# print('serializer.data', serializer.data)
	#
	# 	self.assertEqual(res.data, serializer.data)
	#

	# def test_create_collection(self):
	# 	"""Test creating a collection."""
	# 	payload = {
	# 		'owner': self.user,
	# 		'collection_class': 'place',
	# 		'title': 'Sample POSTed collection',
	# 		'description': 'Created with POST action of ViewSet'
	# 	}
	# 	res = self.client.post(COLLECTIONS_URL, payload)
	#
	# 	self.assertEqual(res.status_code, status.HTTP_201_CREATED)
	# 	collection = Collection.objects.get(id=res.data['id'])
	#
	# 	for k, v in payload.items():
	# 		self.assertEqual(getattr(collection, k), v)
	# 	self.assertEqual(collection.owner, self.user)

# class PrivatePlaceApiTests(TestCase):
# 	""" PLACES: add to dataset """
# 	def setUp(self):
# 		self.client = APIClient()
# 		self.user = create_user(email='user@example.com', password='test123', username='user1')
# 		self.client.force_authenticate(self.user)
# 		self.dataset = create_dataset(user=self.user, label='example_99', title='Example99')
#
# 	def test_create_place(self):
# 		"""Test creating a place in a dataset"""
# 		payload = {
# 			'dataset': self.dataset,
# 			'title': 'Wien',
# 			'src_id': 'abc123',
# 			'ccodes': ['AT']
# 		}
# 		res = self.client.post(PLACES_URL, payload)
#
# 		self.assertEqual(res.status_code, status.HTTP_201_CREATED)
# 		place = Place.objects.get(id=res.data['id'])
#
# 		for k, v in payload.items():
# 			self.assertEqual(getattr(place, k), v)
# 		self.assertEqual(place.dataset, self.dataset)