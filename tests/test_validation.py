# validate tsv/csv and lpf uploads
from django.test import Client
from django.test import TestCase, SimpleTestCase
from django.urls import reverse
import os, codecs, json, mimetypes, re, sys
from chardet import detect
from datasets.static.hashes import mimetypes as mthash
from datasets.utils import validate_tsv

# datasets/create/ ->
# validate_tsv(filepath, extension)

class ValidateTSV(SimpleTestCase):

  def testValidateFiles(self):
    # DatasetCreateView() -> 
    # load file, get encoding, mimetype (file.content_type)
    # validate_tsv(filepath,extension)
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
      #infile = codecs.open(fn, 'r', encoding='utf8')
      encoding = get_encoding_type(fn); print(encoding)
      #encoding = infile.encoding #; print(encoding)
      #mimetype = mimetypes.guess_type(fn, strict=True)[0]; #print(encoding, mimetype)    
      # proceed only if utf-8 and csv or tsv
      #if encoding.lower() in ['utf-8', 'utf8']:
      if encoding and encoding.startswith('UTF-8'):
        mimetype = mimetypes.guess_type(fn, strict=True)[0]; #print(encoding, mimetype)
        if mimetype in mthash.mimetypes:
          ext = mthash.mimetypes[mimetype]
          result = validate_tsv(fn, ext)
          errors.append({"file":f, "msg":result['errors']})
          # validate_tsv adds extension; strip it
          os.rename(fn+'.'+ext,fn)
        else:
          errors.append({"file":f, "msg": "incorrect mimetype: "+mimetype})
          #sys.exit()
      else:
        errors.append({"file":f, "msg": "incorrect encoding: "+str(encoding)})
        #sys.exit()
    print(errors)
    # tests
    self.assertIn('constraint "required" is "True"', errors[0]['msg'][0])
    self.assertIn('not conform to a constraint', errors[0]['msg'][1])
    self.assertEquals(errors[1]['msg'],[])
    self.assertIn('Required field(s) missing', errors[2]['msg'][0])
    self.assertIn('incorrect encoding', errors[3]['msg'])
    #self.assertIn('incorrect mimetype', errors[4]['msg'])
  
  
#for f in files_err:
  #fn = dd+f
  #print(get_encoding_type(fn))
#
# validate_lpf(filepath,'coll')

# DatasetCreateModelForm ->

# datasets/dataset_create.html -> SUBMIT

# DatasetCreateView() -> form_valid()