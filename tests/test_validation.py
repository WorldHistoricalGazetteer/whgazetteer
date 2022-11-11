# validate tsv/csv and lpf uploads
from django.http import HttpResponseRedirect
from django.test import Client
from django.test import TestCase, SimpleTestCase
from django.urls import reverse
import os, codecs, json, mimetypes, re, sys
from chardet import detect
from datasets.static.hashes import mimetypes_plus as mthash_plus
from datasets.utils import validate_tsv

  
class ValidateDelimited(SimpleTestCase):  
  # ok 5 Jan 2021
  # saves spreadsheet as TSV to ./temp, runs validate_tsv() on it
  # TODO: save both spreadsheet and tsv version?
  def testValidateSpreadsheet(self):
    import pandas as pd
    os.chdir('/Users/karlg/Documents/repos/_whgazetteer/')
    dd = '/Users/karlg/Documents/repos/_whgazetteer/_testdata/'
    #files_ok = ['bdda34_xlsx.xlsx','bdda34_ods.ods']
    files = ['bdda34_err_xlsx.xlsx','bdda34_err_ods.ods','bdda34_xlsx.xlsx','bdda34_ods.ods','bdda34_ods_extra-col.ods']
    def get_encoding_excel(file):
      fin = codecs.open(file, 'r')
      return fin.encoding
    
    errors = []
    for f in files:
      fn = dd+f
      mimetype = mimetypes.guess_type(fn, strict=True)[0]
      valid_mime = mimetype in mthash_plus.mimetypes
      if not valid_mime:
        errors.append({"file":f, "msg": "incorrect mimetype: "+mimetype})
        pass
      else:
        if 'spreadsheet' in mimetype:
          encoding = get_encoding_excel(fn)

      if encoding and encoding.lower().startswith('utf-8'):
        ext = mthash_plus.mimetypes[mimetype]

        fnout = dd+'/_temp/'+f
        fout=codecs.open(fnout, 'w', encoding='utf8')
        df = pd.read_excel(fn,converters={'id': str, 'start':str, 'end':str, 'aat_types': str, 'lon': float, 'lat': float})
        header = list(df.columns.values)
        
        table=df.to_csv(sep='\t', index=False).replace('\nan','')
        fout.write(table)
        fout.close()
        result = validate_tsv(fnout, 'tsv')

        errors.append({"file":f, "msg":result['errors']})
      else:
        errors.append({"file":f, "msg": "incorrect encoding: "+str(encoding)})
      print(f, mimetype, encoding)
    print(errors)

    # errors
    self.assertIn('constraint "required" is "True"', errors[0]['msg'][0])
    self.assertIn('constraint "pattern" is', errors[0]['msg'][1])    
    self.assertIn('Required field(s) missing', errors[1]['msg'][0])
    self.assertIn('constraint "required" is "True"', errors[1]['msg'][1])
    self.assertIn('constraint "pattern" is', errors[1]['msg'][2])
    # no errors
    self.assertEquals(errors[2]['msg'],[])
    self.assertEquals(errors[3]['msg'],[])
    self.assertEquals(errors[4]['msg'],[]) # extra column, no errors

  # ok, 4 Jan 2021
  def testValidateTSV(self):
    os.chdir('/Users/karlg/Documents/repos/_whgazetteer/')
    dd = '/Users/karlg/Documents/repos/_whgazetteer/_testdata/'
    #files_ok = ['diamonds135.tsv', 'croniken20.tsv', 'bdda34.csv']
    #files_err = ['bdda34_errors.tsv','bdda34_extra-col.csv','bdda34_missing-col.csv', 'bdda34_utf16.tsv']
    files_err = ['priest_1line.tsv']
    
    def get_encoding_type(file):
      with open(file, 'rb') as f:
        rawdata = f.read()
      return detect(rawdata)['encoding']

    def get_encoding_excel(file):
      fin = codecs.open(file, 'r')
      return fin.encoding
    
    errors = []
    #for f in files:
    for f in files_err:
      fn = dd+f
      mimetype = mimetypes.guess_type(fn, strict=True)[0]
      valid_mime = mimetype in mthash_plus.mimetypes
      if not valid_mime:
        errors.append({"file":f, "msg": "incorrect mimetype: "+mimetype})
        pass
      else:
        if mimetype.startswith('text/'):
          encoding = get_encoding_type(fn)
        elif 'spreadsheet' in mimetype:
          encoding = get_encoding_excel(fn)
      
      if encoding and encoding.lower().startswith('utf-8'):
        ext = mthash_plus.mimetypes[mimetype]
        result = validate_tsv(fn, ext)
        errors.append({"file":f, "msg":result['errors']})
        # validate_tsv() adds extension; strip it
        os.rename(fn+'.'+ext,fn)
      else:
        errors.append({"file":f, "msg": "incorrect encoding: "+str(encoding)})
      
      print(f, mimetype, encoding)
    print(errors)

    # tests
    self.assertIn('constraint "required"', errors[0]['msg'][0]) # missing 'start'
    self.assertIn('constraint "pattern"', errors[0]['msg'][1]) # malformed ccodes (commas)
    self.assertEquals(errors[1]['msg'],[])
    self.assertIn('Required field(s) missing', errors[2]['msg'][0])
    self.assertIn('incorrect encoding', errors[3]['msg'])
  
  
# TODO: validate_lpf(filepath,'coll')

# DatasetCreateModelForm ->
class CallViews(SimpleTestCase):
  def testViews(self):
    responses = []
    urls = ['dashboard', 'datasets:dataset-create']
    param_urls = ['datasets:ds_summary', 'datasets:dataset-delete']
    client = Client()
    for url in urls:
      responses.append( client.get(reverse(url)).status_code )
    
    for url in param_urls:
      responses.append( HttpResponseRedirect(reverse(url, args=(99999,))).status_code )
    
    self.assertEquals(list(set(responses)), [302])

#class ViewTests(TestCase):
  #def datasetCreate(self):
    #dd = '/Users/karlg/Documents/repos/_whgazetteer/_testdata/'
    #files = ['bdda34.csv','bdda34_xlsx.xlsx']
    #url = 'datasets:dataset-create'
    #from django.test import Client
    #from django.shortcuts import get_object_or_404
    #from django.urls import reverse
    #from accounts.models import User
    #user = User.objects.create_user('Satch', password='foo')
    ##user = get_object_or_404(User,pk=14)
    #c = Client()
    #c.login(username='Satch', password='foo')
    #with open(dd+files[0]) as f:
      #response = c.post(reverse(url), {
        #'owner': user.id,
        #'label': 'my-dataset',
        #'title': 'My Dataset',
        #'description': 'blah',
        #'file': f,
        #'datatype': 'place',
        ## dataset_file
        #'rev':1,
        #'format':'delimited',
        #'header': ['a','b'],
        #'df_status': 'uploaded',
        #'numrows': 34,
        #'upload_date': '2021-01-20',
        #'delimiter': '\t'
      #})
    #print('response', response)
  

