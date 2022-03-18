from django.conf import settings
from django.http import HttpResponseRedirect
from django.shortcuts import get_object_or_404, render, redirect
from django.urls import reverse
from django.views.generic import DetailView
from django.utils.safestring import SafeString
from elasticsearch import Elasticsearch
import simplejson as json

from .models import *
#from datasets.models import Dataset

class TraceDetailView(DetailView):
    # 
    template_name = 'traces/trace_detail.html'

    def get_object(self):
        id_ = self.kwargs.get("id")
        print('args',self.args,'kwargs:',self.kwargs)
        es = Elasticsearch([{'host': 'localhost',
                             'port': 9200,
                             'api_key': (settings.ES_APIKEY_ID, settings.ES_APIKEY_KEY),
                             'timeout': 30,
                             'max_retries': 10,
                             'retry_on_timeout': True}])
        q = {"query":{"bool": {"must": [{"match":{"_id": id_}}]}}}
        tid=es.search(index='traces', body=q)['hits']['hits'][0]['_source']['trace_id']
        # tid=es.search(index='traces', doc_type='trace', body=q)['hits']['hits'][0]['_source']['trace_id']
        self.kwargs['tid'] = tid
        return get_object_or_404(Trace, id=tid)

    def get_success_url(self):
        id_ = self.kwargs.get("id")
        return '/traces/'+str(id_)+'/detail'

    def get_context_data(self, *args, **kwargs):
        context = super(TraceDetailView, self).get_context_data(*args, **kwargs)
        es = Elasticsearch([{'host': 'localhost',
                             'port': 9200,
                             'api_key': (settings.ES_APIKEY_ID, settings.ES_APIKEY_KEY),
                             'timeout': 30,
                             'max_retries': 10,
                             'retry_on_timeout': True}])
        id_ = self.kwargs.get("id")
        pid = self.kwargs.get("pid")
        place = get_object_or_404(Place, id=pid)
        context['whg_id'] = id_
        context['payload'] = [] # parent and children if any
        context['traces'] = [] # 

        ids = [pid]
        # get child record ids from index
        q = {"query": {"parent_id": {"type": "child","id":id_}}}
        children = es.search(index='whg', body=q)['hits']
        # children = es.search(index='whg', body=q)['hits']
        for hit in children['hits']:
            ids.append(int(hit['_id']))

        # database records for parent + children into 'payload'
        qs=Place.objects.filter(id__in=ids)
        #print("id_,ids, qs",id_,ids,qs)
        for place in qs:        
            ds = get_object_or_404(Dataset,id=place.dataset.id)
            record = {
          "whg_id":id_,
        "dataset":{"id":ds.id,"label":ds.label},
        "place_id":place.id,
        "src_id":place.src_id, 
        "purl":ds.uri_base+str(place.id) if 'whgaz' in ds.uri_base else ds.uri_base+place.src_id,
        "title":place.title,
        "ccodes":place.ccodes, 
        "names":[name.jsonb for name in place.names.all()], 
        "types":[type.jsonb for type in place.types.all()], 
        "links":[link.jsonb for link in place.links.all()], 
        "geoms":[geom.jsonb for geom in place.geoms.all()],
        "whens":[when.jsonb for when in place.whens.all()], 
        "related":[rel.jsonb for rel in place.related.all()], 
        "descriptions":[descr.jsonb for descr in place.descriptions.all()], 
        "depictions":[depict.jsonb for depict in place.depictions.all()]
      }
            context['payload'].append(record)

        # get traces
        qt = {"query": {"bool": {"must": [{"match":{"body.whg_id": id_ }}]}}}
        trace_hits = es.search(index='traces', body=qt)['hits']['hits']
        # trace_hits = es.search(index='traces', doc_type='trace', body=qt)['hits']['hits']
        # TODO: parse, process
        for h in trace_hits:
            #print('trace hit h',h)
            context['traces'].append({
          'trace_id':h['_id'],
        'target':h['_source']['target'],
        'body':next((x for x in h['_source']['body'] if x['whg_id'] == id_), None),
        'bodycount':len(h['_source']['body'])
      })

        print('context payload',str(context['payload']))
        print('context traces',str(context['traces']))
        return context
