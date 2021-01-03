# validate tsv/csv and lpf uploads
from django.test import Client
from django.urls import reverse
import os, codecs, json, mimetypes, re, sys
from datasets.static.hashes import mimetypes as mthash
from datasets.utils import validate_tsv

client = Client()
# datasets/create/ ->
response = client.get(reverse('datasets:dataset-create')); print('status:',response.status_code)
os.chdir('/Users/karlg/Documents/Repos/_whgazetteer/')

# DatasetCreateView() -> 
# load file, get encoding, mimetype (file.content_type)
# validate_tsv(filepath,extension)
dd = '/Users/karlg/repos/_whgazetteer/_testdata/'
#files = ['diamonds135.tsv', 'croniken20.tsv', 'bdda34.csv', 'bdda34.xlsx', 'bdda34.ods']
files = ['diamonds135.tsv', 'croniken20.tsv', 'bdda34.csv']
files_err = ['bdda34_errors.csv','bdda34_extra-col.csv','bdda34_missing-col.csv']

errors = []
#for f in files:
for f in files_err:
  fn = dd+f
  infile = codecs.open(fn, 'r')
  encoding = infile.encoding

  # proceed only if utf-8 and csv or tsv
  if encoding.lower() in ['utf-8', 'utf8']:
    mimetype = mimetypes.guess_type(fn, strict=True)[0]; #print(encoding, mimetype)
    if mimetype in mthash.mimetypes:
      ext = mthash.mimetypes[mimetype]
      result = validate_tsv(fn, ext)
      errors.append({"file":f, "msg":result['errors']})
      #print(fn,result)
    else:
      errors.append({"type": "mimetype", "msg": "incorrect mimetype: "+mimetype})
      sys.exit()
  else:
    errors.append({"type": "encoding", "msg": "incorrect encoding: "+encoding})
    sys.exit()
  # validate_tsv adds extension; strip it
  os.rename(fn+'.'+ext,fn)
print('errors',errors)

#
# validate_lpf(filepath,'coll')

# DatasetCreateModelForm ->

# datasets/dataset_create.html -> SUBMIT

# DatasetCreateView() -> form_valid()