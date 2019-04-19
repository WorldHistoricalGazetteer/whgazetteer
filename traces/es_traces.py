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
              #id=whg_id, 
              body=rec)
      count +=1
    except:
      print(rec['id'], ' broke it')
      print("error:", sys.exc_info())
  print(str(count)+' records indexed')

def init():
  global es, idx, rows, trdata
  idx = 'traces'
  file = 'examples_whg.json'
  import os, codecs, time, datetime, json,sys
  mappings = codecs.open('static/mappings_traces.json', 'r', 'utf8').read()
  os.chdir('/Users/karlg/Documents/Repos/linked-traces-format/')

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
    print ('index "'+idx+'" created')
  except Exception as ex:
    print(ex)

  index_traces(trdata)

init()

#def makeRanges(doc):
  #for anno in doc:
    #for body in anno['body']:
      ##print(len(body['when']))
      #for ts in body['when']:
        #print('start',arr_start,'end',arr_end)

