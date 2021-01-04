# validate tsv/csv and lpf uploads
from django.http import HttpResponseRedirect
from django.test import Client
from django.test import TestCase, SimpleTestCase
from django.urls import reverse
import os, codecs, json, mimetypes, re, sys
from chardet import detect
from datasets.static.hashes import mimetypes as mthash
from datasets.utils import validate_tsv

# DatasetCreateView() -> 
# load file, get encoding, mimetype (file.content_type)
# validate_tsv(filepath,extension)

class ValidateTSV(SimpleTestCase):
  # test OK 3 Jan 2021
  def testValidateFiles(self):
    os.chdir('/Users/karlg/Documents/repos/_whgazetteer/')
    dd = '/Users/karlg/Documents/repos/_whgazetteer/_testdata/'
    #files = ['diamonds135.tsv', 'croniken20.tsv', 'bdda34.csv', 'bdda34.xlsx', 'bdda34.ods']
    #files = ['diamonds135.tsv', 'croniken20.tsv', 'bdda34.csv']
    files_err = ['bdda34_errors.csv','bdda34_extra-col.csv','bdda34_missing-col.csv',
                 'bdda34.ods','bdda34_utf16.tsv']
    
    def get_encoding_type(file):
      with open(file, 'rb') as f:
        rawdata = f.read()
      return detect(rawdata)['encoding']
        
    errors = []
    #for f in files:
    for f in files_err:
      fn = dd+f
      mimetype = mimetypes.guess_type(fn, strict=True)[0]; #print(encoding, mimetype)
      encoding = get_encoding_type(fn); print(encoding)
      # proceed only if (csv or tsv) and utf8
      if mimetype in mthash.mimetypes:
        if encoding and encoding.startswith('UTF-8'):
          ext = mthash.mimetypes[mimetype]
          result = validate_tsv(fn, ext)
          errors.append({"file":f, "msg":result['errors']})
          # validate_tsv() adds extension; strip it
          os.rename(fn+'.'+ext,fn)
        else:
          errors.append({"file":f, "msg": "incorrect encoding: "+str(encoding)})
      else:
        errors.append({"file":f, "msg": "incorrect mimetype: "+mimetype})
    #print(errors)

    # tests
    self.assertIn('constraint "required" is "True"', errors[0]['msg'][0])
    self.assertIn('not conform to a constraint', errors[0]['msg'][1])
    self.assertEquals(errors[1]['msg'],[])
    self.assertIn('Required field(s) missing', errors[2]['msg'][0])
    self.assertIn('incorrect mimetype', errors[3]['msg'])
    self.assertIn('incorrect encoding', errors[4]['msg'])
  
  
# TODO: validate_lpf(filepath,'coll')

# DatasetCreateModelForm ->
class CallViews(SimpleTestCase):
  def testViews(self):
    responses = []
    urls = ['dashboard', 'datasets:dataset-create']
    param_urls = ['datasets:dataset-detail', 'datasets:dataset-delete']
    client = Client()
    for url in urls:
      responses.append( client.get(reverse(url)).status_code )
    
    for url in param_urls:
      responses.append( HttpResponseRedirect(reverse(url, args=(99999,))).status_code )
    
    self.assertEquals(list(set(responses)), [302])

# datasets/dataset_create.html -> SUBMIT
# DatasetCreateView() -> form_valid()


#for f in files_err:
  # fn = dd+f
  # print(get_encoding_type(fn))
