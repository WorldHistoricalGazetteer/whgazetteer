from django.shortcuts import render, get_object_or_404, redirect
import codecs, tempfile, os, re, sys #ipdb,
import simplejson as json
from datasets.models import *
import pandas as pd

# diamonds/543; starts with diamonds135_sa51t6f.tsv
# updates with diamonds135_rev2.tsv

ds=get_object_or_404(Dataset,pk=543) #diamonds
wd = '/Users/karlg/Documents/Repos/_whgazetteer/'
file_a = ds.files.all().order_by('-rev')[1]
file_b = ds.files.all().order_by('-rev')[0]
fn_a = file_a.file.name
fn_b = file_b.file.name
adf = pd.read_csv(wd+'media/'+fn_a,delimiter='\t')
bdf = pd.read_csv(wd+'media/'+fn_b,delimiter='\t')
ids_a = adf['id'].tolist()
ids_b = bdf['id'].tolist()
resobj={"count_new":len(ids_b),'count_diff':len(ids_b)-len(ids_a)}
# new or removed columns?
col_del = list(set(adf.columns)-set(bdf.columns))
col_add = list(set(bdf.columns)-set(adf.columns))
resobj['col_add']=col_add
resobj['col_del']=col_del
resobj['rows_add']=list(set(ids_b)-set(ids_a))
resobj['rows_del']=list(set(ids_a)-set(ids_b))
text='The revised dataset has '+str(resobj['count_new'])+' records, a difference of '+str(resobj['count_diff'])+". Columns "
text += 'to add: '+str(resobj['col_add']) + '. 'if len(resobj['col_add']) > 0 else \
            'to remove: '+ str(resobj['col_del'])+'. ' if len(resobj['col_del']) > 0 \
            else "remain the same. "
text += 'Records to be added: '+str(resobj['rows_add'])+'. ' if len(resobj['rows_add'])>0 else ''
text += 'Records to be removed: '+str(resobj['rows_del'])+'. ' if len(resobj['rows_del'])>0 else ''
text += 'All records with an ID matching one in the existing dataset will be replaced.'
resobj['text'] = text
print(text)