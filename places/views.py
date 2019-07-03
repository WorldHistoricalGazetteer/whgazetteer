from django.http import HttpResponseRedirect, JsonResponse, HttpResponse
from django.shortcuts import render, get_object_or_404, redirect
from django.urls import reverse
from django.views.generic import DetailView
from django.utils.safestring import SafeString
from elasticsearch import Elasticsearch
import simplejson as json

from .models import *
from datasets.models import Dataset

def placeFull(request,id):
  response = HttpResponse(content_type='text/json')
  response['Content-Disposition'] = 'attachment;filename="place-full.json"'
  
  #template_name = 'places/place_portal.html'
  print('request,id',request,id)
  def get_object(self):
    id_ = self.kwargs.get("id")
    print('args',self.args,'kwargs:',self.kwargs)
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    q = {"query":{"bool": {"must": [{"match":{"_id": id_}}]}}}
    pid=es.search(index='whg', doc_type='place', body=q)['hits']['hits'][0]['_source']['place_id']
    self.kwargs['pid'] = pid
    return get_object_or_404(Place, id=pid)

  #def get_success_url(self):
    #id_ = self.kwargs.get("id")
    #return '/places/'+str(id_)+'/detail'
  
  def minmax(timespans):
    starts = sorted([t['start']['in'] for t in timespans])
    ends = sorted([t['end']['in'] for t in timespans])
    #minmax = {'start':min(starts), 'end':max(ends)}    
    minmax = [min(starts), max(ends)]

  def get_context_data(self, *args, **kwargs):
    context = super(placeFull, self).get_context_data(*args, **kwargs)
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    id_ = self.kwargs.get("id")
    pid = self.kwargs.get("pid")
    place = get_object_or_404(Place, id=pid)
    context['whg_id'] = id_
    context['payload'] = [] # parent and children if any
    context['traces'] = [] # 

    ids = [pid]
    # get child record ids from index
    q = {"query": {"parent_id": {"type": "child","id":id_}}}
    children = es.search(index='whg', doc_type='place', body=q)['hits']
    for hit in children['hits']:
      ids.append(int(hit['_id']))

    # database records for parent + children into 'payload'
    qs=Place.objects.filter(id__in=ids).order_by('-whens__minmax')
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
        "types":[t.jsonb for t in place.types.all()], 
        "links":[link.jsonb for link in place.links.all()], 
        "geoms":[geom.jsonb for geom in place.geoms.all()],
        "whens":[when.jsonb for when in place.whens.all()], 
        "related":[rel.jsonb for rel in place.related.all()], 
        "descriptions":[descr.jsonb for descr in place.descriptions.all()], 
        "depictions":[depict.jsonb for depict in place.depictions.all()]
      }
      context['payload'].append(record)
      
    print('payload',context['payload'])
    # get traces
    qt = {"query": {"bool": {"must": [{"match":{"body.whg_id": id_ }}]}}}
    trace_hits = es.search(index='traces', doc_type='trace', body=qt)['hits']['hits']
    # TODO: parse, process
    for h in trace_hits:
      #print('trace hit h',h)
      context['traces'].append({
        'trace_id':h['_id'],
        'target':h['_source']['target'],
        'body':next((x for x in h['_source']['body'] if x['whg_id'] == id_), None),
        'bodycount':len(h['_source']['body'])
      })
    
    response.write(context['payload'])
    response.write(context['traces'])
    return response

class PlacePortalView(DetailView):
  template_name = 'places/place_portal.html'

  def get_object(self):
    id_ = self.kwargs.get("id")
    print('args',self.args,'kwargs:',self.kwargs)
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    q = {"query":{"bool": {"must": [{"match":{"_id": id_}}]}}}
    pid=es.search(index='whg', doc_type='place', body=q)['hits']['hits'][0]['_source']['place_id']
    self.kwargs['pid'] = pid
    return get_object_or_404(Place, id=pid)

  def get_success_url(self):
    id_ = self.kwargs.get("id")
    return '/places/'+str(id_)+'/detail'
  
  def minmax(timespans):
    starts = sorted([t['start']['in'] for t in timespans])
    ends = sorted([t['end']['in'] for t in timespans])
    #minmax = {'start':min(starts), 'end':max(ends)}    
    minmax = [min(starts), max(ends)]

  def get_context_data(self, *args, **kwargs):
    context = super(PlacePortalView, self).get_context_data(*args, **kwargs)
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    id_ = self.kwargs.get("id")
    pid = self.kwargs.get("pid")
    place = get_object_or_404(Place, id=pid)
    context['whg_id'] = id_
    context['payload'] = [] # parent and children if any
    context['traces'] = [] # 

    ids = [pid]
    # get child record ids from index
    q = {"query": {"parent_id": {"type": "child","id":id_}}}
    children = es.search(index='whg', doc_type='place', body=q)['hits']
    for hit in children['hits']:
      ids.append(int(hit['_id']))

    # database records for parent + children into 'payload'
    qs=Place.objects.filter(id__in=ids).order_by('-whens__minmax')
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
        "types":[t.jsonb for t in place.types.all()], 
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
    trace_hits = es.search(index='traces', doc_type='trace', body=qt)['hits']['hits']
    # TODO: parse, process
    for h in trace_hits:
      #print('trace hit h',h)
      context['traces'].append({
        'trace_id':h['_id'],
        'target':h['_source']['target'],
        'body':next((x for x in h['_source']['body'] if x['whg_id'] == id_), None),
        'bodycount':len(h['_source']['body'])
      })
    
    #print('context payload',str(context['payload']))
    #print('context traces',str(context['traces']))
    return context

class PlaceContribView(DetailView):
  template_name = 'places/place_contrib.html'

  def get_success_url(self):
    id_ = self.kwargs.get("id")
    return '/contrib/'+str(id_)+'/detail'

  def get_object(self):
    print('kwargs:',self.kwargs)
    id_ = self.kwargs.get("id")
    return get_object_or_404(Place, id=id_)

  def get_context_data(self, *args, **kwargs):
    context = super(PlaceContribView, self).get_context_data(*args, **kwargs)
    id_ = self.kwargs.get("id")
    place = get_object_or_404(Place, id=id_)
    spinedata = Dataset.objects.filter(id__in=[1,2])

    context['names'] = place.names.all()
    context['links'] = place.links.all()
    context['whens'] = place.whens.all()
    context['geoms'] = place.geoms.all()
    context['types'] = place.types.all()
    context['related'] = place.related.all()
    context['descriptions'] = place.descriptions.all()
    context['depictions'] = place.depictions.all()

    context['spine'] = spinedata
    #print('place context',str(context))
    return context
