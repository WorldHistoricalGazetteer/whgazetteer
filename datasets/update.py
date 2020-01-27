from django.shortcuts import render, get_object_or_404, redirect
import simplejson as json
import codecs, tempfile, os, re, sys 
from places.models import *
from datasets.models import Dataset, Hit, DatasetFile
from datasets.utils import validate_lpf, goodtable

dsid=516 # 'never' lugares_60_1.jsonld
ds = get_object_or_404(Dataset,pk=dsid)
places = Place.objects.filter(dataset=ds.label).values_list('src_id')

datadir='/Users/karlg/Documents/Repos/_whgazetteer/example_data/'
# jsonld FeatureCollection
# validate new
fnn = datadir+'lugares_60_1_rev2.jsonld'
fin_n = codecs.open(fnn, 'r', 'utf8')
result_n = validate_lpf(fin_n,'coll')
# re-open !?
fin_n = codecs.open(fnn, 'r', 'utf8')
raw_n = json.loads(fin_n.read())

fin_n.close()


#os.chdir('/Users/karlg/Documents/Repos/_whgazetteer/')
src_id=f['@id'] if 'http' not in f['@id'] and len(f['@id']) < 25 \
  else re.search("(\/|=)(?:.(?!\/|=))+$",f['@id']).group(0)[1:]