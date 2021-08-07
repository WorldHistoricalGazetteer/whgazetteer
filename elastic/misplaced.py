from elasticsearch import Elasticsearch      
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

from elastic.es_utils import esq_pid, esq_parent

# 81228 (Alexandria, VA)
# walk through rearranging docs
def misplaced(pid, idx):
  doc = es.search(index=idx, body=esq_pid(pid))
  print(doc)
  
  
  


misplaced(81228, 'whg')


# parent and its children, by _id/whg_id (should be same)
GET /whg/_search
{"query":{"bool":{"should": [
    {"parent_id": {"type": "child","id":"14159046"}},
    {"match":{"_id":"14159046"}}
]}}}
