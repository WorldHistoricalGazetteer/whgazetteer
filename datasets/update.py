from django.shortcuts import render, get_object_or_404, redirect
import simplejson as json
import codecs, tempfile, os, re, sys 
from places.models import *
from datasets.models import Dataset, Hit, DatasetFile
from datasets.utils import validate_lpf, goodtable

dsid=461 # 'places_p169b' current file: user_A_User/P169_out_YnA0XAl.tsv
ds = get_object_or_404(Dataset,pk=dsid)
oldids = list(Place.objects.filter(dataset=ds.label).values_list('src_id',flat=True))

# FILES (new has been validated [0], current became [1])
cur_fileobj = DatasetFile.objects.filter(dataset_id_id=dsid).order_by('-upload_date')[1]
cur_filename = cur_fileobj.file.name
cur_uribase = cur_fileobj.uri_base

new_fileobj = DatasetFile.objects.filter(dataset_id_id=dsid).order_by('-upload_date')[0]
new_filename = new_fileobj.file.name
new_uribase = new_fileobj.uri_base

datadir='/Users/karlg/Documents/Repos/_whgazetteer/media/'
# jsonld FeatureCollection
# validate new
fn = datadir+cur_filename
fin_n = codecs.open(fnn, 'r', 'utf8')
result_n = validate_lpf(fin_n,'coll')
# re-open !?
fin_n = codecs.open(fnn, 'r', 'utf8')
feats_new = json.loads(fin_n.read())['features']
fin_n.close()

def sameRecord(old,new):
    
newids, newfeats = [],[]
for f in feats_new:
    newid=f['@id'] if uribase == None else f['@id'].replace(uribase,'')
    newids.append(newid); print(newid)
    if newid in oldids:
        newfeats
        # compare
        oldplace = get_object_or_404(Place, src_id=newid, dataset=ds.label)
        
        
missing = list(set(oldids)-set(newids)) # remove from WHG
new = list(set(newids)-set(oldids)) # add to WHG
changes = '??'

#os.chdir('/Users/karlg/Documents/Repos/_whgazetteer/')
#src_id=f['@id'] if 'http' not in f['@id'] and len(f['@id']) < 25 \
  #else re.search("(\/|=)(?:.(?!\/|=))+$",f['@id']).group(0)[1:]