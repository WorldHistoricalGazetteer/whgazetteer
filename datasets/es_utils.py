# es_utils.py 7 Feb 2019; rev 5 Mar 2019
# misc supporting eleasticsearch tasks (es.py)

def esInit(idx):
    import os, codecs, time, datetime
    os.chdir('/Users/karlg/Documents/Repos/_whgdata')

    from elasticsearch import Elasticsearch
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    mappings = codecs.open('data/elastic/mappings/mappings_whg.json', 'r', 'utf8').read()

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
        
def maxID(es):
    q={"query": {"bool": {"must" : {"match_all" : {}} }},
       "sort": [{"whg_id": {"order": "desc"}}],
       #"sort": [{"_id": {"order": "desc"}}],
       "size": 1  
       }
    res = es.search(index='whg', body=q)
    if len(res['hits']['hits']) > 0:
        maxy = int(res['hits']['hits'][0]['_id'])
    else:
        maxy = 10000000
    return maxy

def uriMaker(place):
    from django.shortcuts import get_object_or_404
    from datasets.models import Dataset
    ds = get_object_or_404(Dataset,id=place.dataset.id)
    if ds.uri_base.startswith('http://whgazetteer'):
        return ds.uri_base + str(place.id)
    else:
        return ds.uri_base + str(place.src_id)
    
def findMatch(qobj,es):
    matches = {"parents":[], "names":[]}
    q_links_f = {"query": { 
     "bool": {
       "must": [
         {"terms": {"links.identifier": qobj['links'] }}
        ]
     }
    }}
    
    if len(qobj['links']) > 0: # if links, terms query
        res = es.search(index='whg', doc_type='place', body=q_links_f)
        hits = res['hits']['hits']
        if len(hits) > 0:
            for h in hits:
                #print(h['_source']['names'])
                matches['parents'].append( h['_id'] )
                #matches['parents'].append( h['_source']['place_id'] )
                for n in h['_source']['names']:
                    matches['names'].append(n['toponym'])
        # else: create seed (and/or parent+child)
    return matches

def makeDoc(place,parentid):
    cc_obj = {
        "relation": {},
        "children": [],
        "suggest": {"input":[]},
        "minmax": [],
        "place_id": place.id,
        "dataset": place.dataset.label,
        "src_id": place.src_id,
        "title": place.title,
        "uri": uriMaker(place),
        "ccodes": place.ccodes,
        "names": parsePlace(place,'names'),
        "types": parsePlace(place,'types'),
        "geoms": parsePlace(place,'geoms'),
        "links": parsePlace(place,'links'),
        "timespans": [],
        "descriptions": parsePlace(place,'descriptions'),
        "depictions": [], 
        "relations": []
    }
    return cc_obj

def parsePlace(place,attr):
    qs = eval('place.'+attr+'.all()')
    arr = []
    for obj in qs:
        if attr == 'geoms':
            g = obj.jsonb
            geom={"location":{"type":g['type'],"coordinates":g['coordinates']}}
            if 'citation' in g.keys(): geom["citation"] = g['citation']
            if 'geowkt' in g.keys(): geom["geowkt"] = g['geowkt']
            arr.append(geom)
        else:
            arr.append(obj.jsonb)
    return arr

def jsonDefault(value):
    import datetime
    if isinstance(value, datetime.date):
        return dict(year=value.year, month=value.month, day=value.day)
    else:
        return value.__dict__

def deleteDocs(ids):
    from elasticsearch import Elasticsearch
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    for i in ids:
        try:
            es.delete(index='whg', doc_type='place', id=i)
        except:
            print('failed delete for: ',id)
            pass
        
def deleteKids(ids):
    from elasticsearch import Elasticsearch
    {"nested": {
            "path": "is_conflation_of",
            "query": 
              {"nested" : {
                "path" :  "is_conflation_of.types",
                "query" : {"terms": {"is_conflation_of.place_id": ids}}
                }
              }
          }}    
    q={"query": {"terms": { "":ds }}}
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    for i in ids:
        try:
            es.delete(index='whg', doc_type='place', id=i)
        except:
            print('failed delete for: ',id)
            pass

def deleteDataset(ds):
    q={"query": {"match": { "seed_dataset":ds }}}
    try:
        es.delete(es_index='whg', doc_type='place', body=q)
    except:
        print('failed delete for: ',ds)
        pass
    



def queryObject(place):
    from datasets.utils import hully
    qobj = {"place_id":place.id,"src_id":place.src_id,"title":place.title}
    variants=[]; geoms=[]; types=[]; ccodes=[]; parents=[]; links=[]
    
    # ccodes (2-letter iso codes)
    for c in place.ccodes:
        ccodes.append(c)
    qobj['ccodes'] = place.ccodes
    
    # types (Getty AAT identifiers)
    for t in place.types.all():
        types.append(t.jsonb['identifier'])
    qobj['types'] = types
    
    # names
    for name in place.names.all():
        variants.append(name.toponym)
    qobj['variants'] = variants
    
    # parents
    for rel in place.related.all():
        if rel.json['relation_type'] == 'gvp:broaderPartitive':
            parents.append(rel.jsonb['label'])
    qobj['parents'] = parents
    
    # links
    if len(place.links.all()) > 0:
        for l in place.links.all():
            links.append(l.jsonb['identifier'])
        qobj['links'] = links
    
    # geoms
    if len(place.geoms.all()) > 0:
        geom = place.geoms.all()[0].jsonb
        if geom['type'] in ('Point','MultiPolygon'):
            qobj['geom'] = place.geoms.first().jsonb
        elif geom['type'] == 'MultiLineString':
            qobj['geom'] = hully(geom)
    
    return qobj

def makeSeed(place, dataset, whgid):
    # whgid, place_id, dataset, src_id, title
    sobj = SeedPlace(whgid, place.id, dataset, place.src_id, place.title )
    
    # pull from name.json
    for n in place.names.all():
        sobj.suggest['input'].append(n.json['toponym'])
    
    # no place_when data yet
    if len(place.whens.all()) > 0:
        sobj['minmax'] = []
    
    sobj.is_conflation_of.append(makeChildConflate(place))
    
    return sobj

# abandoned for makeDoc()
class SeedPlace(object):
    def __init__(self, whgid, place_id, dataset, src_id, title):
        self.whgid = whgid
        self.representative_title = title
        self.seed_dataset = dataset
        self.representative_point = []
        self.representative_shape = []
        self.suggest = {"input":[]}
        self.minmax = []
        self.is_conflation_of = []

    def __str__(self):
        import json
        #return str(self.__class__) + ": " + str(self.__dict__)    
        return json.dumps(self.__dict__)

    def toJSON(self):
        import json
        return json.dumps(self, default=jsonDefault, sort_keys=True, indent=2)            

class IndexedPlaceFlat(object):
    def __init__(self, whg_id, place_id, dataset, src_id, title, uri):
        self.relation = {"name":"parent"}
        self.children = []
        self.suggest = {"input":[]}
        self.representative_point = []
        self.minmax = []

        self.whg_id = whg_id
        self.place_id = place_id
        self.dataset = dataset
        self.src_id = src_id
        self.title = title
        self.uri = uri

        self.ccodes = []
        self.names = []
        self.types = []
        self.geoms = []
        self.links = []
        self.timespans = []
        self.descriptions = []
        self.depictions = []
        self.relations = []
        
    def __str__(self):
        import json
        #return str(self.__class__) + ": " + str(self.__dict__)    
        return json.dumps(self.__dict__)

    def toJSON(self):
        import json
        return json.dumps(self, default=lambda o: o.__dict__, 
                          sort_keys=True, indent=2)    

# to be used in subsequent adds to is_conflation_of[]
class MatchRecord(object):
    def __init__(self, dataset, id, title, uri, exact):
        self.id = id
        self.title = title
        self.uri = uri
        self.source_gazetteer = dataset
        self.exact_matches = exact
        self.names = [{"name":title,"language": ""}]
        self.temporal_bounds = ["", "", "", "", ""]
    
    def __str__(self):
        import json
        return json.dumps(self.__dict__)    