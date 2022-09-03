"""
Tests only for remote API endpoints
added Sep 2022 @kgeographer
"""
import os

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from rest_framework import status
from rest_framework.test import APIClient

from datasets.models import Dataset
from places.models import Place
from collection.models import Collection

from remote.serializers import  (
	DatasetRemoteSerializer, DatasetRemoteDetailSerializer, PlaceRemoteSerializer, CollectionRemoteSerializer
)

DATASETS_URL = reverse('remote:dataset-list')
def dataset_detail_url(dataset_id):
	"""Create and return a dataset detail URL."""
	return reverse('remote:dataset-detail', args=[dataset_id])

COLLECTIONS_URL = reverse('remote:collection-list')
def collection_detail_url(collection_id):
	"""Create and return a collection detail URL."""
	return reverse('remote:collection-detail', args=[collection_id])

PLACES_URL = reverse('remote:place-list')

def create_user(**params):
	"""Create and return a new user."""
	return get_user_model().objects.create_user(**params)

def create_dataset(user, **params):
	"""Create and return a sample dataset; params incl. unique label"""
	defaults = {
		'title': 'Sample recipe title',
		'description': 'Sample description',
		'public': False,
		'numrows': 99
	}
	defaults.update(params)

	dataset = Dataset.objects.create(owner=user, **defaults)
	return dataset

def create_collection(user, **params):
	"""Create and return a sample collection w/required fields"""
	defaults = {
		'title': 'Sample collection title',
		'description': 'Sample collection description',
		# 'keywords': ['how', 'about', 'these']
	}
	defaults.update(params)

	collection = Collection.objects.create(owner=user, **defaults)
	return collection


"""Test unauthenticated API requests fail"""
class PublicAPITests(TestCase):
	def setUp(self):
		self.client = APIClient()

	def test_auth_required_ds(self):
		res = self.client.get(DATASETS_URL)

		self.assertEqual(res.status_code, status.HTTP_401_UNAUTHORIZED)

	def test_auth_required_coll(self):
		res = self.client.get(COLLECTIONS_URL)

		self.assertEqual(res.status_code, status.HTTP_401_UNAUTHORIZED)


"""Test authenticated API requests."""
class PrivateApiTests(TestCase):
	""" DATASETS: list, detail, create """
	def setUp(self):
		self.client = APIClient()
		self.user = create_user(email='user@example.com', password='test123', username='user1')
		self.client.force_authenticate(self.user)

	def test_retrieve_datasets(self):
		"""Test retrieving a list of all datasets"""
		create_dataset(user=self.user, label="example_1")
		create_dataset(user=self.user, label="example_2")

		res = self.client.get(DATASETS_URL)

		datasets = Dataset.objects.all().order_by('-id')
		serializer = DatasetRemoteSerializer(datasets, many=True)
		# print('res.data.results', res.data.get('results'))
		# print('serializer.data', serializer.data)

		self.assertEqual(res.status_code, status.HTTP_200_OK)
		self.assertEqual(res.data.get('results'), serializer.data)

	def test_dataset_list_limited_to_user(self):
		"""Test list of datasets is limited to authenticated user."""
		other_user = create_user(email='other@example.com', password='test123', username='user2')
		# Datasets must have unique labels
		create_dataset(user=other_user, label='example3')
		create_dataset(user=self.user, label='example4')

		res = self.client.get(DATASETS_URL)

		datasets = Dataset.objects.filter(owner=self.user)
		serializer = DatasetRemoteSerializer(datasets, many=True)
		self.assertEqual(res.status_code, status.HTTP_200_OK)
		self.assertEqual(res.data.get('results'), serializer.data)

	def test_get_dataset_detail(self):
		"""Test get dataset detail"""
		dataset = create_dataset(user=self.user, label="example_5" )

		url = dataset_detail_url(dataset.id)
		res = self.client.get(url)

		serializer = DatasetRemoteDetailSerializer(dataset)

		self.assertEqual(res.data, serializer.data)

	def test_create_dataset(self):
		"""Test creating a dataset."""
		payload = {
			'owner': self.user,
			'title': 'Sample POSTed dataset',
			'label': 'example_6',
			'description': 'Created with POST action of ViewSet'
		}
		res = self.client.post(DATASETS_URL, payload)

		self.assertEqual(res.status_code, status.HTTP_201_CREATED)
		dataset = Dataset.objects.get(id=res.data['id'])
		for k, v in payload.items():
			self.assertEqual(getattr(dataset, k), v)
		self.assertEqual(dataset.owner, self.user)


	""" COLLECTIONS: list, detail, create, update (add place) """
	def test_retrieve_collections(self):
		"""Test retrieving a list of all collections"""
		create_collection(user=self.user)
		create_collection(user=self.user)

		res = self.client.get(COLLECTIONS_URL)

		collections = Collection.objects.all().order_by('-id')
		serializer = CollectionRemoteSerializer(collections, many=True)

		self.assertEqual(res.status_code, status.HTTP_200_OK)
		self.assertEqual(res.data.get('results'), serializer.data)

	def test_collection_list_limited_to_user(self):
		"""Test list of collections is limited to authenticated user."""
		other_user = create_user(email='other@example.com', password='test123', username='user3')
		create_collection(user=other_user)
		create_collection(user=self.user)

		res = self.client.get(COLLECTIONS_URL)

		collections = Collection.objects.filter(owner=self.user)
		serializer = CollectionRemoteSerializer(collections, many=True)

		self.assertEqual(res.status_code, status.HTTP_200_OK)
		self.assertEqual(res.data.get('results'), serializer.data)

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

	def test_create_collection(self):
		"""Test creating a collection."""
		payload = {
			'owner': self.user,
			'collection_class': 'place',
			'title': 'Sample POSTed collection',
			'description': 'Created with POST action of ViewSet'
		}
		res = self.client.post(COLLECTIONS_URL, payload)

		self.assertEqual(res.status_code, status.HTTP_201_CREATED)
		collection = Collection.objects.get(id=res.data['id'])

		for k, v in payload.items():
			self.assertEqual(getattr(collection, k), v)
		self.assertEqual(collection.owner, self.user)

class PrivatePlaceApiTests(TestCase):
	""" PLACES: add to dataset """
	def setUp(self):
		self.client = APIClient()
		self.user = create_user(email='user@example.com', password='test123', username='user1')
		self.client.force_authenticate(self.user)
		self.dataset = create_dataset(user=self.user, label='example_99', title='Example99')

	def test_create_place(self):
		"""Test creating a place in a dataset"""
		payload = {
			'dataset': self.dataset,
			'title': 'Wien',
			'src_id': 'abc123',
			'ccodes': ['AT']
		}
		res = self.client.post(PLACES_URL, payload)

		self.assertEqual(res.status_code, status.HTTP_201_CREATED)
		place = Place.objects.get(id=res.data['id'])

		for k, v in payload.items():
			self.assertEqual(getattr(place, k), v)
		self.assertEqual(place.dataset, self.dataset)