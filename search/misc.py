# some queries 12 Feb 2019
from django.conf import settings
import json, codecs

# working out geo_shape indexing
def init_geotest():
    global es, idx, rows
    idx = 'geotest' 
    import json, codecs, os
    from elasticsearch7 import Elasticsearch
    es = Elasticsearch([{'host': 'localhost',
                         'port': 9200,
                         'api_key': (settings.ES_APIKEY_ID, settings.ES_APIKEY_KEY),
                         'timeout': 30,
                         'max_retries': 10,
                         'retry_on_timeout': True}])
    os.chdir('/Users/karlg/Documents/Repos/_whgdata')
    mappings = codecs.open('data/elastic/mappings/mappings_geo2.json', 'r', 'utf8').read()
    
    try:
        es.indices.delete(idx)
    except Exception as ex:
        print(ex)
    try:
        es.indices.create(index=idx, ignore=400, body=mappings)
        print ('index "'+idx+'" created')
    except Exception as ex:
        print(ex)
init_geotest()
line_obj = {'title': 'Drau', 'geoms': [{'type': 'MultiLineString', 'coordinates': [[[15.828857421875, 46.4545352234316], [15.94228515625, 46.3825869812441]],[[-18.396875, 64.7361514343691], [-18.4327880859375, 64.6947695984316]]]}]}
res = es.index(index='geotest', id=98765, body=line_obj)
point_obj = {'title': 'Drau', 'geoms': [{'type': 'Point', 'coordinates': [12, 60]}]}
res = es.index(index='geotest', id=98766, body=point_obj)
plus_obj = {'title': 'Pushy', 'geoms': [
    {'location':{'type': 'Point', 'coordinates': [16, 46]},
    'foo':'bar'}
]}
res = es.index(index='geotest', id=98767, body=plus_obj)


## *** ##
idx="whg_flat"
def findName():
    name = input('name [Calusa]: ') or 'Calusa'
    #print('name: ',name)
    all_hits=[]
    q_suggest = { 
        "suggest":{"suggest":{"prefix":name,"completion":{"field":"suggest"}}}
    }
    res = es.search(index=idx, body=q_suggest)
    # build all_hits[]
    hits = res['suggest']['suggest'][0]['options']
    #print('hits:',hits)
    if len(hits) > 0:
        for h in hits:
            hit_id = h['_id']
            if 'parent' in h['_source']['relation'].keys():
                print('forget it, not a parent')
                ## it's a child, get siblings and add to all_hits[]
                #pid = h['_source']['relation']['parent']
                ##print('child, has parent:',pid)
                #q_parent = {"query":{"parent_id":{"type":"child","id":pid}}}
                #res = es.search(index='whg_flat', body=q_parent)
                #kids = res['hits']['hits']
                #for k in kids:
                    #all_hits.append(k['_source'])
            else:
                # it's a parent, add to all_hits[] and get kids if any
                all_hits.append(h['_source'])
                q_parent = {"query":{"parent_id":{"type":"child","id":hit_id}}}
                res = es.search(index='whg_flat', body=q_parent)
                if len(res['hits']['hits']) > 0:
                    #print('parent has kids:',str(res['hits']))
                    for i in res['hits']['hits']:
                        all_hits.append(i['_source'])
        print(json.dumps(all_hits,indent=2))
        print('got '+str(len(all_hits))+' results, like any?\n')
    else:
        print('got nothing for that string, sorry!')

findName()