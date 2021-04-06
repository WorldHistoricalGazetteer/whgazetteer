# update ES records for this and that

from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

from datasets.models import Dataset
from places.models import Place

three = ['euratlas_cities','owtrad','pleiades20k']
#qs = Place.objects.all().filter(indexed=True,dataset__in=three) #19452
qs = Place.objects.all().filter(indexed=True,dataset='pleiades20k') #
count = 0
for p in qs:
    #print(p.id,p.title)
    count +=1
    q_update = { "script": {
        "source": """
            ctx._source.fclasses = params.fclasses; 
        """,
        "lang": "painless",
        "params":{"fclasses": p.fclasses}
    },
    "query": {"match":{"place_id": p.id}}}
    es.update_by_query(index='whg', doc_type='place', body=q_update, conflicts='proceed')
print(str(count)+' updated')