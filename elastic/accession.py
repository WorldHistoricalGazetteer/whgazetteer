# test functions related to accessioning a dataset
# 16 Aug 2022

from copy import deepcopy
from elastic.es_utils import *
from elasticsearch7 import Elasticsearch, RequestError
es = settings.ES_CONN
idx='whg'
# owt10 places
 #/'Memphis',
 # 'Dzhukuchak',
 #/'Barskon',
 #/'Barskoon',
 #/ 'Chong-Kyzylsu',
 # 'Santash',
 # 'Burana',
 #/ 'Fderik',
 #/ 'Antakya',
 # 'Al Madain'

#update 14158663 children
# correct: ["81403","13041394","6713134","14090523"]
# or return to previous: ["81403","13041394"]
qfix = {"script": {
  "source": "ctx._source.children = params.kids; ctx._source.relation.remove('whg_id')",
  "lang": "painless",
  "params": {
    "kids": ["81403","13041394"]
  }
}, "query": {"match": {"whg_id": 14158663}}}
try:
  es.update_by_query(index=idx, body=qfix, conflicts='proceed')
except RequestError as rq:
  print('Error: ', rq.error, rq.info)

# restore 14090523 after task delete
try:
  es.delete(index='whg', id='14090523')
  es.index(index='whg', id='14090523', body=srcd, routing=1)
except RequestError as rq:
  print('reindex failed (demoted)')
  print('Error: ', rq.error, rq.info)

srcd={'types': [{'identifier': '300008389',
   'sourceLabel': 'inhabited place',
   'label': 'cities'},
  {'identifier': '300008347',
   'sourceLabel': 'archaeological site',
   'label': 'inhabited places'},
  {'identifier': '300000810',
   'sourceLabel': 'city',
   'label': 'archaeological sites'}],
 'searchy': ['Hatay', 'Antakyé', 'Antakya', 'Antakiyah'],
 'src_id': '7594295',
 'minmax': [],
 'suggest': {'input': ['Hatay', 'Antakyé', 'Antakya', 'Antakiyah']},
 'title': 'Antakya',
 'geoms': [{'geowkt': 'POINT(36.1167 36.2333)',
   'location': {'coordinates': [36.1167, 36.2333], 'type': 'Point'}}],
 'uri': 'http://whgazetteer.org/api/places/5545349',
 'descriptions': [],
 'relation': {'name': 'parent'},
 'names': [{'toponym': 'Hatay',
   'citation': {'id': 'http://vocab.getty.edu/page/tgn/7594295',
    'label': 'Getty TGN Dec 2017'}},
  {'toponym': 'Antakyé',
   'citation': {'id': 'http://vocab.getty.edu/page/tgn/7594295',
    'label': 'Getty TGN Dec 2017'}},
  {'toponym': 'Antakya',
   'citation': {'id': 'http://vocab.getty.edu/page/tgn/7594295',
    'label': 'Getty TGN Dec 2017'}},
  {'toponym': 'Antakiyah',
   'citation': {'id': 'http://vocab.getty.edu/page/tgn/7594295',
    'label': 'Getty TGN Dec 2017'}}],
 'depictions': [],
 'children': [],
 'ccodes': ['TR'],
 'fclasses': ['P', 'S'],
 'links': [{'identifier': 'tgn:7594295', 'type': 'closeMatch'}],
 'timespans': [],
 'relations': [],
 'dataset': 'tgn_filtered_01',
 'place_id': 5545349,
 'whg_id': 14090523}

# align_idx(dsid)
#   for place in ds.places.all()
#     build_qobj(pid)
#     es_lookup_whg(qobj)
#       run q0a, then q0b
#         > hitlist (hl)
#         hlAnalyze(pid, hl)
#           addChild() or demoteParent()
#     write Hit records w/normalized json fields 
#    
# use by groom_idx.py
# pids = [place_id] all indexed places for a dataset
# groomy(pids)
#   for pid in pids:
#     doc = es.search(body = match place_id)['hits']['hits'][0]
#     profile = profileHit(doc)
#     if profile['links]:
#       hitlist = es.search(profile['links])
#       append all to profiles[]
#       processProfiles(pid, profiles)




# used in accessioning, not grooming
def addChild(place, parent_id):
  print('adding', place, 'as child of', parent_id)
  # child_doc = makeDoc(place)
  # relation = {'name':'child', 'parent': parent_id}
  # child_doc['relation'] = relation	
  # ** refactor tasks.align_idx::1716 **
  #_id == pid
  #add _id to parent children[]
  #add variants to parent searchy[]

def demoteParents(demoted, winner_id):
  # demoted = ['14090523'] (whg_ids)
  # winner_id '14158663' (whg_id)
  print('demoteParents()',demoted, winner_id)
  
  # updates 'winner' with children & names from demoted
  def q_updatewinner(kids, names):
    return {"script":{
      "source": """ctx._source.children.addAll(params.newkids);
      ctx._source.suggest.input.addAll(params.names);
      ctx._source.searchy.addAll(params.names);
      """,
      "lang": "painless",
      "params":{
        "newkids": kids,
        "names": names }
    }}

  for d in demoted:
    #d = demoted[0]
    # test 20220815
    #d = '14090523' (TGN Antakya, pid 545349)
    #winner_id = '14158663' (Black Antioch, pid 81401)

    # get the demoted doc, its names and kids if any
    qget = """{"query": {"bool": {"must": [{"match":{"_id": "%s" }}]}}}"""    
    try:      
      qget = qget % (d)
      doc = es.search(index='whg', body=qget)['hits']['hits'][0]
    except RequestError as rq:
      print('failed getting winner; winner_id, pid',winner_id)
      print('Error: ', rq.error, rq.info)

    srcd = doc['_source']
    kids = srcd['children']
    # add this doc b/c it's now a kid
    kids.append(doc['_id'])
    names = list(set(srcd['suggest']['input']))
    
    # first update the 'winner' parent
    q=q_updatewinner(kids, names)
    try:
      es.update(index=idx, id=winner_id, body=q)
    except RequestError as rq:
      print('q_updatewinner failed (winner_id)',winner_id)
      print('Error: ', rq.error, rq.info)

    # then modify copy of demoted,
    # delete the old, index the new
    # --------------
    newsrcd = deepcopy(srcd)
    newsrcd['relation'] = {"name":"child","parent":winner_id}
    newsrcd['children'] = []
    if 'whg_id' in newsrcd:
      newsrcd.pop('whg_id')
    # zap the old demoted, index the modified
    try:      
      es.delete(index='whg', id=d)
      es.index(index='whg',id=d,body=newsrcd,routing=1)
    except RequestError as rq:
      print('reindex failed (demoted)',d)
      print('Error: ', rq.error, rq.info)

def topParent(parents, form):
  #print('topParent():', parents)   
  if form == 'set':
    # if eq # of kids, use lowest _id
    parents.sort(key=lambda x:(-x[1], x[0]))
    top = parents[0][0]
  else:
    # a list of external parent _ids
    # get one with most children, or just the first?
    top = parents[0]
  #print('winner_id is', top)
  return top

# HITLIST EXAMPLES 
# case3: (6595829) parent/child plus child w/external parent 
hl3 = [
  {
      "_id": "14125428",
        "pid": 5580275,
      "title": "Toru\u0144",
      "pass": "pass0",
      "links": [
        "tgn:7007831"
          ],
      "role": "parent",
      "children": [
        "90272"
      ]
      },
    {
      "_id": "90272",
        "pid": 90272,
      "title": "Thorn",
      "pass": "pass0",
      "links": [
        "tgn:7007831"
          ],
      "role": "child",
      "children": [],
      "parent": "14125428"
      },
    {
      "_id": "6370246",
        "pid": 6370246,
      "title": "Toru\u0144",
      "pass": "pass0",
      "links": [
        "wd:Q47554",
          "gn:3083271",
        "viaf:149128250"
        ],
      "role": "child",
      "children": [],
      "parent": "14154739"
    }
]
# case2: (6595984) 2 parents, both with same kid 
hl2 = [
  {
    "_id": "88534",
    "pid": 88534,
    "title": "Pskov",
    "pass": "pass0",
    "links": [
      "dbp:Pskov",
      "tgn:7010261"
    ],
    "role": "child",
    "children": [],
    "parent": "12841451"
  },
  {
    "_id": "12841451",
    "pid": 6099720,
    "title": "Pskov",
    "pass": "pass0",
    "links": [
      "tgn:7010261"
    ],
    "role": "parent",
    "children": [
      "88534"
    ]
  },
  {
    "_id": "14153017",
    "pid": 88535,
    "title": "Pskov",
    "pass": "pass0b",
    "links": [
      "dbp:Pskov"
    ],
    "role": "parent",
    "children": [
      "88534"
    ]
  }
]

# case1: (6595825) 1 parent w/2 children
hl1 = [
  {
      "_id": "86746",
     "pid": 86746,
     "title": "Marienburg",
     "pass": "pass0",
     "links": [
       "gn:3092472",
         "wd:Q35723337",
         "dbp:Malbork",
         "tgn:7007740"
         ],
     "role": "child",
     "children": [],
     "parent": "13511942"
     },
    {
    "_id": "13511942",
      "pid": 4966395,
    "title": "Malbork",
    "pass": "pass0",
    "links": [
      "tgn:7007740"
        ],
    "role": "parent",
    "children": [
      "86746",
        "6370425"
    ]
    },
  {
    "_id": "6370425",
      "pid": 6370425,
    "title": "Malbork",
    "pass": "pass0",
    "links": [
      "viaf:168429967",
        "loc:n50056727",
      "wd:Q146820",
      "viaf:146067891",
      "gn:7531264",
      "gn:3092472"
      ],
    "role": "child",
    "children": [],
    "parent": "13511942"
  }
]

    #q_demote = {"script":{
      #"source":"ctx._source.relation.name=params.name;ctx._source.relation.parent=params.parent",
      #"lang": "painless",
      #"params":{
        #"name":"child",
        #"parent": winner_id}
    #}}
    #q_demote_ubq = {"script": {
      #"source": """ctx._source.relation=params.relation;ctx._source.children=[];""", 
      #"lang": "painless", 
      #"params": {
        #"relation": {"name": "child", "parent": winner_id}
      #}, 
      #"query": {"match": {"_id": d}}}}
    
    # then modify and replace demoted
    #es.update(idx, int(d), q_demote)
    # Document mapping type name can't start with '_'
    
    #es.update('whg', d, q_demote)
    # '[place][14156468]: document missing'
    
    #es.update_by_query(idx, body=q_demote_ubq)
    # 
    
#es.index(index='whg',id=winner_id,body=src_w)
