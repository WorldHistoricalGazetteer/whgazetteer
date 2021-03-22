# functions for accessioning a dataset
# 20 Mar 2021

from es.es_utils import *
from elasticsearch import Elasticsearch      
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# used by datasets.align_whg_testy(), eventually: 
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

def addChild(place, parent_id):
  print('adding', place, 'as child of', parent_id)
  # child_doc = makeDoc(place)
  # relation = {'name':'child', 'parent': parent_id}
  # child_doc['relation'] = relation	
  # ** refactor tasks.align_idx::1716 **
  #_id == pid
  #add _id to parent children[]
  #add variants to parent searchy[]

def demoteParents(_ids, newparent_id):
  for _id in _ids:
    print('demoting', _id, 'to child of', newparent_id, '(& its kids to siblings)')
  # makes _id a child of newparent_id
  # relation = {'name':'child', 'parent': newparent_id}
  # adds its children[] to newparent_id
  # adds its variants to newparent_id.searchy[] 
  #then de-dupe it
  # empties pe.searchy[]

def topParent(parents, form):
  print('topParent():', parents)
  if form == 'set':
    # if eq # of kids, use lowest _id
    parents.sort(key=lambda x:(-x[1], x[0]))
    top = parents[0][0]
  else:
    # a list of external parent _ids
    # get one with most children, or just the first?
    top = parents[0]
  print('winner_id is', top)
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

