from django.shortcuts import render, get_object_or_404, redirect
import codecs, tempfile, os, re, sys #ipdb,
import simplejson as json
from datasets.models import *
import pandas as pd

# diamonds/543; starts with diamonds135_sa51t6f.tsv
# updates with diamonds135_rev2.tsv

ds=get_object_or_404(Dataset,pk=543) #diamonds
wd = '/Users/karlg/Documents/Repos/_whgazetteer/'
file_a = DatasetFile.objects.filter(dataset_id_id=ds.id).order_by('-rev')[1]
file_b = DatasetFile.objects.filter(dataset_id_id=ds.id).order_by('-rev')[0]
fn_a = file_a.file.name
fn_b = file_b.file.name
adf = pd.read_csv(wd+'media/'+fn_a,delimiter='\t')
bdf = pd.read_csv(wd+'media/'+fn_b,delimiter='\t')
ids_a = adf['id'].tolist()
ids_b = bdf['id'].tolist()
asum={"count":len(ids_a),"cols":list(adf.columns)}
bsum={"count":len(ids_b),"cols":list(bdf.columns)}



