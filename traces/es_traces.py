import sys
def index_traces(trdata):
  print('data is:',type(trdata))
  count=0
  for rec in trdata:
    try:
      del rec['@context'] # not needed in index
      res = es.index(
        index=idx, 
            doc_type='trace',
              body=rec)
      count +=1
    except:
      print(rec['id'], ' broke it')
      print("error:", sys.exc_info())
  print(str(count)+' records indexed')

def init():
  global es, idx, rows, trdata
  wd = '/Users/karlg/Documents/Repos/_whgazetteer/es/'
  idx = 'traces03'
  file = wd+'trace_data/traces_20200702.json'
  import os, codecs, time, datetime, json,sys
  mappings = codecs.open(wd+'mappings_traces03.json', 'r', 'utf8').read()

  from elasticsearch import Elasticsearch
  es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

  # read from file 
  infile = codecs.open(file, 'r', 'utf8')
  trdata = json.loads(infile.read())

  # zap existing if exists, re-create
  try:
    es.indices.delete(idx)
  except Exception as ex:
    print(ex)
  try:
    es.indices.create(index=idx, ignore=400, body=mappings)
    #es.indices.create(index=idx, ignore=400)
    print ('index "'+idx+'" created')
  except Exception as ex:
    print(ex)

  #index_traces(trdata)

init()


