from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect
from django.views.generic import DetailView

from datetime import datetime
from elasticsearch import Elasticsearch
import itertools

from places.models import Place
from datasets.models import Dataset

class PlacePortalView(DetailView):
  template_name = 'places/place_portal.html'

  # //
  # given index id (whg_id) returned by typeahead/suggest, get its db record (a parent);
  # build array of place_ids (parent + children);
  # iterate those to build payload;
  # create addl context values from set
  # //

  def get_object(self):
    id_ = self.kwargs.get("id")
    print('args',self.args,'kwargs:',self.kwargs)
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    q = {"query":{"bool": {"must": [{"match":{"_id": id_}}]}}}
    #q = {"query":{"bool": {"must": [{"match":{"place_id": id_}}]}}}
    pid=es.search(index='whg02', doc_type='place', body=q)['hits']['hits'][0]['_source']['place_id']
    self.kwargs['pid'] = pid
    return get_object_or_404(Place, id=pid)

  def get_success_url(self):
    id_ = self.kwargs.get("id")
    return '/places/'+str(id_)+'/detail'
  
  
  def get_context_data(self, *args, **kwargs):
    print('get_context_data kwargs',self.kwargs)
    def mm(attrib):
      # names, geoms, types, relations, whens
      extent=[]
      for a in attrib:
        minmax=[]
        if 'when' in a:
          starts = sorted([t['start']['in'] for t in a['when']['timespans']])
          # TODO: this could throw error if >1 timespan
          ends = sorted([t['end']['in'] for t in a['when']['timespans']]) \
            if 'end' in a['when']['timespans'][0] else [datetime.now().year]
          minmax = [int(min(starts)), int(max(ends))]
          if len(minmax)>0: extent.append(minmax)
        elif 'timespans' in a:
          #print('place portal context a in attrib',a)
          starts = sorted(
            [(t['start']['in'] if 'in' in t['start'] else t['start']['earliest']) for t in a['timespans']]
          )
          ends = sorted(
            [(t['end']['in'] if 'in' in t['end'] else t['end']['latest']) for t in a['timespans']]
          )
          minmax = [int(min(starts)), int(max(ends))]        
          if len(minmax)>0: extent.append(minmax)
      return extent

    context = super(PlacePortalView, self).get_context_data(*args, **kwargs)
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    id_ = self.kwargs.get("id")
    pid = self.kwargs.get("pid")
    place = get_object_or_404(Place, id=pid)
    context['whg_id'] = id_
    context['payload'] = [] # parent and children if any
    context['traces'] = [] # 
    # place portal headers gray for records from these
    context['core'] = ['gn500','gnmore','ne_countries','ne_rivers982','ne_mountains','wri_lakes','tgn_filtered_01']

    ids = [pid]
    # get child record ids from index
    q = {"query": {"parent_id": {"type": "child","id":id_}}}
    children = es.search(index='whg02', doc_type='place', body=q)['hits']
    for hit in children['hits']:
      ids.append(int(hit['_id']))

    # database records for parent + children into 'payload'
    qs=Place.objects.filter(id__in=ids).order_by('-whens__minmax')
    context['title'] = qs.first().title
    
    for place in qs:
      ds = get_object_or_404(Dataset,id=place.dataset.id)
      extents = []
      # isolate temporal scoping where exists; build summing object
      whens = [when.jsonb for when in place.whens.all()]     
      names = [name.jsonb for name in place.names.all()]
      #print('names in PlacePortalView',names)
      geoms = [geom.jsonb for geom in place.geoms.all()]
      types = [t.jsonb for t in place.types.all()]
      related = [rel.jsonb for rel in place.related.all()]
      timespans = list(t for t,_ in itertools.groupby(place.timespans)) if place.timespans else []
      # data object for summing temporality of all attestations for a place
      # TODO: leaving relations out b/c when for lugares is ill-formed
      # cf. 20190416_lugares-lpf.sql, line 63
      extents += mm(names),mm(geoms),mm(types),mm(whens),mm(related)
      #extents += mm(names),mm(geoms),mm(types),mm(whens)
      
      record = {
        "whg_id":id_,
        "dataset":{"id":ds.id,"label":ds.label,"name":ds.title},
        "place_id":place.id,
        "src_id":place.src_id, 
        "purl":ds.uri_base+str(place.id) if 'whgaz' in ds.uri_base else ds.uri_base+place.src_id,
        "title":place.title,
        "ccodes":place.ccodes, 
        "whens":whens, 
        "names":names, 
        "geoms":geoms,
        "types":types, 
        "related":related, 
        "links":[link.jsonb for link in place.links.distinct('jsonb') if not link.jsonb['identifier'].startswith('whg')], 
        "descriptions":[descr.jsonb for descr in place.descriptions.all()], 
        "depictions":[depict.jsonb for depict in place.depictions.all()],
        "minmax":place.minmax,
        "timespans":timespans
      }
      context['payload'].append(record)
      context['extents'] = extents
    #TODO: compute global minmax for payload
    #print('payload',context['payload'])
    print('names',record['names'])
    
    # GET TRACES; e.g. whg_id(id_) = 13040977 for Khotan (a child); pid = 6135435
    # but traces have place_id; we have child ids already
    print('ids',ids)
    qt = {"query": {"bool": {"must": [  {"terms":{"body.place_id": ids }}]}}}
    #qt = {"query": {"bool": {"must": [{"match":{"body.whg_id": id_ }}]}}}
    trace_hits = es.search(index='traces', doc_type='trace', body=qt)['hits']['hits']
    # TODO: parse, process
    for h in trace_hits:
      print('trace hit h',h)
      context['traces'].append({
        'trace_id':h['_id'],
        'target':h['_source']['target'][0] if type(h['_source']['target']) == list else h['_source']['target'],
        #'body':next((x for x in h['_source']['body'] if x['place_id'] == id_), None),
        'body':next((x for x in h['_source']['body'] if x['place_id'] ), None),
        'bodycount':len(h['_source']['body'])
      })
    
    return context

class PlaceFullView(PlacePortalView):
  def render_to_response(self, context, **response_kwargs):
    return JsonResponse(context, **response_kwargs)

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
