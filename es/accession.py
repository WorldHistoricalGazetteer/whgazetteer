# functions for grooming, accessioning a dataset
# 22 Mar 2021

from copy import deepcopy
from es.es_utils import *
from elasticsearch import Elasticsearch      
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
idx='whg'
# use by datasets.align_whg_testy(), eventually: 
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
  #demoted = ['14156468']
  #newparent_id = winner_id
  qget = """{"query": {"bool": {"must": [{"match":{"_id": "%s" }}]}}}"""
  
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
    # get the demoted doc, its names and kids if any
    #d = demoted[0]
    #d = '14156468'
    #winner_id = '14156467'
    qget = qget % (d)
    doc = es.search(index='whg', body=qget)['hits']['hits'][0]
    src = doc['_source']
    kids = src['children']
    # add this doc b/c it's now a kid
    kids.append(doc['_id'])
    names = list(set(src['suggest']['input']))
    
    # first update the 'winner' parent
    q=q_updatewinner(kids, names)
    es.update(idx,winner_id,body=q,doc_type='place')


    q_demote = {"script":{
      "source":"ctx._source.relation.name=params.name;ctx._source.relation.parent=params.parent",
      "lang": "painless",
      "params":{
        "name":"child",
        "parent": winner_id}
    }}
    q_demote_ubq = {"script": {
      "source": """ctx._source.relation=params.relation;ctx._source.children=[];""", 
      "lang": "painless", 
      "params": {
        "relation": {"name": "child", "parent": winner_id}
      }, 
      "query": {"match": {"_id": d}}}}
    
    # then modify and replace demoted
    #es.update(idx, int(d), q_demote)
    # Document mapping type name can't start with '_'
    
    es.update('whg', d, q_demote, doc_type='place')
    # '[place][14156468]: document missing'
    
    es.update_by_query(idx, body=q_demote_ubq)
    # 
    
    # delete, reindex approach
    # update and update_by_query DO NOT work
    # --------------
    #newsrc = deepcopy(src)
    #newsrc['relation'] = {"name":"child","parent":winner_id}
    #newsrc['children'] = []
    #if 'whg_id' in newsrc:
      #newsrc.pop('whg_id')
    ## zap the old, index the modified
    #es.delete('whg',winner_id,doc_type='place')
    #es.index(index='whg',doc_type='place',id=d,body=newsrc,routing=1)
    #es.index(index='whg',doc_type='place',id=winner_id,body=src_w)

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

