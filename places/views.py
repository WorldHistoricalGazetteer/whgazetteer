from django.http import JsonResponse
from django.shortcuts import get_object_or_404
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
    pid=es.search(index='whg', doc_type='place', body=q)['hits']['hits'][0]['_source']['place_id']
    self.kwargs['pid'] = pid
    return get_object_or_404(Place, id=pid)

  
  def get_context_data(self, *args, **kwargs):
    print('get_context_data kwargs',self.kwargs)
    def mm(attrib):
      #print('attrib in PlacePortalView.mm()', attrib)
      extent=[]
      for a in attrib:
        minmax=[]
        if 'when' in a: # i.e. names, geoms, types, related
          starts = sorted([t['start']['in'] for t in a['when']['timespans']])
          # TODO: this could throw error if >1 timespan
          ends = sorted([t['end']['in'] for t in a['when']['timespans']]) \
            if 'end' in a['when']['timespans'][0] else [datetime.now().year]
          minmax = [int(min(starts)), int(max(ends))]
          if len(minmax)>0: extent.append(minmax)
        elif 'timespans' in a: # i.e. whens
          # object in LP v1.0 datasets, list in +=v1.1
          if type(a['timespans']==dict): 
            a['timespans'] = [a['timespans']]
          starts = sorted(
            [(t['start']['in'] if 'in' in t['start'] else t['start']['earliest']) for t in a['timespans'][0]]
          )
          ends = sorted(
            [(t['end']['in'] if 'in' in t['end'] else t['end']['latest']) for t in a['timespans'][0]]
          )
          minmax = [int(min(starts)), int(max(ends))]
          #print('starts, ends, minmax',starts,ends,minmax)
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
    context['allts'] = [] # agg timespans from records
    # place portal headers gray for records from these
    context['core'] = ['gn500','gnmore','ne_countries','ne_rivers982','ne_mountains','wri_lakes','tgn_filtered_01']

    ids = [pid]
    # get child record ids from index
    q = {"query": {"parent_id": {"type": "child","id":id_}}}
    children = es.search(index='whg', doc_type='place', body=q)['hits']
    for hit in children['hits']:
      ids.append(int(hit['_id']))

    # database records for parent + children into 'payload'
    qs=Place.objects.filter(id__in=ids).order_by('-whens__minmax')
    context['title'] = qs.first().title
    extents=[]
    for place in qs:
      ds = get_object_or_404(Dataset,id=place.dataset.id)
      whens = [when.jsonb for when in place.whens.all()]     
      names = [name.jsonb for name in place.names.all()]
      geoms = [geom.jsonb for geom in place.geoms.all()]
      types = [t.jsonb for t in place.types.all()]
      related = [rel.jsonb for rel in place.related.all()]
      
      # 
      timespans = list(t for t,_ in itertools.groupby(place.timespans)) if place.timespans else []
      context['allts'] += timespans
      # data object summing temporality of all attestations for a place
      # TODO: leaving relations out b/c when for lugares is ill-formed
      # cf. 20190416_lugares-lpf.sql, line 63
      extents += mm(names),mm(geoms),mm(types),mm(whens),mm(related)
      #extents += timespans
      #extents += mm(names),mm(geoms),mm(types),mm(whens)
      #print('extents',extents)
      
      record = {
        "whg_id":id_,
        "dataset":{"id":ds.id,"label":ds.label,"name":ds.title,"webpage":ds.webpage},
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


    #TODO: compute global minmax for payload
    #print('payload',context['payload'])
    #print('whens',record['whens'])
    
    def mm_trace(tsarr):
      if tsarr==[]:
        return ''
      else:
        #print('mm_trace() tsarr',tsarr)
        # TODO: not only simple years here; sorts string years?
        starts = sorted( [t['start'] for t in tsarr] )
        ends = sorted( [t['end'] for t in tsarr] )
        mm = [min(starts), max(ends)]
        mm = sorted(list(set([min(starts), max(ends)])))
        return '('+str(mm[0])+('/'+str(mm[1]) if len(mm)>1 else '')+')'  
    
    # get traces for this index parent and its children
    #print('ids',ids)
    qt = {"query": {"bool": {"must": [  {"terms":{"body.place_id": ids }}]}}}
    trace_hits = es.search(index='traces', doc_type='trace', body=qt)['hits']['hits']
    # for each hit, get target and aggregate body relation/when
    for h in trace_hits:
      #print('trace hit h',h)
      # filter bodies for place_id
      bods=[b for b in h['_source']['body'] if b['place_id'] in ids]
      # agg "relation (start/end)" of bodies
      #print('bods',bods)
      # {'id': 'http://whgazetteer.org/place/174101', 'title': 'Santiago de Cuba', 'place_id': 174101, 'relations': [{'when': [{'end': '1519-02-10', 'start': '1519-02-10'}], 'relation': 'waypoint'}]}
      bod = {
        "id": bods[0]['id'],
        "title": bods[0]['title'],
        "place_id": bods[0]['place_id'],
        "relations": [x['relations'][0]['relation'] +' '+mm_trace(x['relations'][0]['when']) for x in bods]
      }      
      context['traces'].append({
        'trace_id':h['_id'],
        'target':h['_source']['target'][0] if type(h['_source']['target']) == list else h['_source']['target'],
        'body': bod,
        'bodycount':len(h['_source']['body'])
      })
    
    return context

# TODO: used?
class PlaceModalView(DetailView):
  template_name = 'places/place_modal.html'

  # //
  # given index id (whg_id), build abbreviated version of place portal
  # //

  def get_object(self):
    id_ = self.kwargs.get("id")
    print('args',self.args,'kwargs:',self.kwargs)
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    q = {"query":{"bool": {"must": [{"match":{"_id": id_}}]}}}
    #q = {"query":{"bool": {"must": [{"match":{"place_id": id_}}]}}}
    pid=es.search(index='whg', doc_type='place', body=q)['hits']['hits'][0]['_source']['place_id']
    self.kwargs['pid'] = pid
    return get_object_or_404(Place, id=pid)

  
  def get_context_data(self, *args, **kwargs):
    print('get_context_data kwargs',self.kwargs)
    #def mm(attrib):
      ##print('attrib in PlacePortalView.mm()', attrib)
      #extent=[]
      #for a in attrib:
        #minmax=[]
        #if 'when' in a: # i.e. names, geoms, types, related
          #starts = sorted([t['start']['in'] for t in a['when']['timespans']])
          ## TODO: this could throw error if >1 timespan
          #ends = sorted([t['end']['in'] for t in a['when']['timespans']]) \
            #if 'end' in a['when']['timespans'][0] else [datetime.now().year]
          #minmax = [int(min(starts)), int(max(ends))]
          #if len(minmax)>0: extent.append(minmax)
        #elif 'timespans' in a: # i.e. whens
          ## object in LP v1.0 datasets, list in +=v1.1
          #if type(a['timespans']==dict): 
            #a['timespans'] = [a['timespans']]
          #starts = sorted(
            #[(t['start']['in'] if 'in' in t['start'] else t['start']['earliest']) for t in a['timespans'][0]]
          #)
          #ends = sorted(
            #[(t['end']['in'] if 'in' in t['end'] else t['end']['latest']) for t in a['timespans'][0]]
          #)
          #minmax = [int(min(starts)), int(max(ends))]
          ##print('starts, ends, minmax',starts,ends,minmax)
          #if len(minmax)>0: extent.append(minmax)
      #return extent

    context = super(PlaceModalView, self).get_context_data(*args, **kwargs)
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    id_ = self.kwargs.get("id")
    pid = self.kwargs.get("pid")
    place = get_object_or_404(Place, id=pid)
    context['whg_id'] = id_
    context['intervals'] = []
    context['payload'] = [] # parent and children if any
    context['traces'] = [] # 
    # place portal headers gray for records from these
    context['core'] = ['gn500','gnmore','ne_countries','ne_rivers982','ne_mountains','wri_lakes','tgn_filtered_01']

    ids = [pid]
    # get child record ids from index
    q = {"query": {"parent_id": {"type": "child","id":id_}}}
    children = es.search(index='whg', doc_type='place', body=q)['hits']
    for hit in children['hits']:
      ids.append(int(hit['_id']))

    # database records for parent + children into 'payload'
    qs=Place.objects.filter(id__in=ids).order_by('-whens__minmax')
    context['title'] = qs.first().title
    for place in qs:
      ds = get_object_or_404(Dataset,id=place.dataset.id)

      # isolate temporal scoping where exists; build summing object
      whens = [when.jsonb for when in place.whens.all()]     
      names = [name.jsonb for name in place.names.all()]
      geoms = [geom.jsonb for geom in place.geoms.all()]
      types = [t.jsonb for t in place.types.all()]
      related = [rel.jsonb for rel in place.related.all()]
      timespans = list(t for t,_ in itertools.groupby(place.timespans)) if place.timespans else []
      print('timespans',timespans)
      # data object summing temporality of all attestations for a place
      # TODO: leaving relations out b/c when for lugares is ill-formed
      # cf. 20190416_lugares-lpf.sql, line 63
      #extents += mm(names),mm(geoms),mm(types),mm(whens),mm(related)
      #extents += mm(names),mm(geoms),mm(types),mm(whens)
      
      record = {
        "whg_id":id_,
        "dataset":{"id":ds.id,"label":ds.label,"name":ds.title,"webpage":ds.webpage},
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
      context['intervals'].append(timespans)
    
    def mm_trace(tsarr):
      if tsarr==[]:
        return ''
      else:
        #print('mm_trace() tsarr',tsarr)
        # TODO: not only simple years here; sorts string years?
        starts = sorted( [t['start'] for t in tsarr] )
        ends = sorted( [t['end'] for t in tsarr] )
        mm = [min(starts), max(ends)]
        mm = sorted(list(set([min(starts), max(ends)])))
        return '('+str(mm[0])+('/'+str(mm[1]) if len(mm)>1 else '')+')'  
    
    # get traces for this index parent and its children
    #print('ids',ids)
    qt = {"query": {"bool": {"must": [  {"terms":{"body.place_id": ids }}]}}}
    trace_hits = es.search(index='traces', doc_type='trace', body=qt)['hits']['hits']
    # for each hit, get target and aggregate body relation/when
    for h in trace_hits:
      #print('trace hit h',h)
      # filter bodies for place_id
      bods=[b for b in h['_source']['body'] if b['place_id'] in ids]
      # agg "relation (start/end)" of bodies
      #print('bods',bods)
      # {'id': 'http://whgazetteer.org/place/174101', 'title': 'Santiago de Cuba', 'place_id': 174101, 'relations': [{'when': [{'end': '1519-02-10', 'start': '1519-02-10'}], 'relation': 'waypoint'}]}
      bod = {
        "id": bods[0]['id'],
        "title": bods[0]['title'],
        "place_id": bods[0]['place_id'],
        "relations": [x['relations'][0]['relation'] +' '+mm_trace(x['relations'][0]['when']) for x in bods]
      }      
      context['traces'].append({
        'trace_id':h['_id'],
        'target':h['_source']['target'][0] if type(h['_source']['target']) == list else h['_source']['target'],
        'body': bod,
        'bodycount':len(h['_source']['body'])
      })
    
    return context


class PlaceFullView(PlacePortalView):
  def render_to_response(self, context, **response_kwargs):
    return JsonResponse(context, **response_kwargs)

