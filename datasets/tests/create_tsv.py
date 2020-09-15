# test dataset create w/tsv, step-by-step
#
from django.test import TestCase
from django.contrib.auth.models import User
from datasets.models import Dataset,DatasetFile
from places.models import *
from django.shortcuts import get_object_or_404

from datasets.views import DatasetCreateView, ds_insert_tsv
from datasets.utils import validate_tsv

# create Dataset
dsobj = Dataset.objects.create(
    
)
# load a file

# validate it

# create datasetFile
DatasetFile.objects.create(
    dataset_id = dsobj,
    file = 'user_SomeUser/'+filename,
    rev = 1,
    format = 'delimited',
    delimiter = "n/a",
    df_status = 'format_ok',
    upload_date = None,
    header = result['columns'] if "columns" in result.keys() else [],
    numrows = result['count']
)

