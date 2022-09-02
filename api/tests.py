"""
Tests for remote API endpoints
Sep 2022 @kgeographer
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

from api.remoteserializers import  (
	DatasetRemoteSerializer, DatasetRemoteDetailSerializer, PlaceRemoteSerializer, CollectionRemoteSerializer
)

DATASETS_URL = reverse('api:dataset-list')

def dataset_detail_url(dataset_id):
	"""Create and return a recipe detail URL."""
	return reverse('api:dataset-detail', args=[dataset_id])

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

	# print('defaults post-update', defaults)
	dataset = Dataset.objects.create(owner=user, **defaults)
	return dataset

# ds=create_dataset(user, label="example_6")

class PublicDatasetAPITests(TestCase):
	"""Test unauthenticated API requests fail"""

	def setUp(self):
		self.client = APIClient()

	def test_auth_required(self):
		"""Test auth is required to call API."""
		res = self.client.get(DATASETS_URL)

		self.assertEqual(res.status_code, status.HTTP_401_UNAUTHORIZED)

class PrivateDatasetApiTests(TestCase):
	"""Test authenticated API requests."""
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
		print('res.data', res.data)
		print('serializer.data', serializer.data)
		self.assertEqual(res.data, serializer.data)