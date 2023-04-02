from django.conf import settings
from django.contrib.auth import get_user_model
User = get_user_model()
from django.http import JsonResponse,HttpResponseRedirect
from django.shortcuts import get_object_or_404
from django.views.generic import DetailView

from datetime import datetime
from elasticsearch7 import Elasticsearch
import itertools, re

from collection.models import Collection
from datasets.models import Dataset
from places.models import Place
from places.utils import attribListFromSet

# write review status = 2 (per authority)
def defer_review(request, pid, auth, last):
  print('defer_review() pid, auth, last', pid, auth, last)
  p = get_object_or_404(Place, pk=pid)
  if auth in ['whg','idx']:
    p.review_whg = 2
  elif auth.startswith('wd'):
    p.review_wd = 2
  else:
    p.review_tgn = 2
  p.save()
  referer = request.META.get('HTTP_REFERER')
  base = re.search('^(.*?)review', referer).group(1)
  print('referer',referer)
  print('last:',int(last))
  if '?page' in referer:
    nextpage=int(referer[-1])+1
    if nextpage < int(last):
      # there's a next record/page
      return_url = referer[:-1] + str(nextpage)
    else:
      return_url = base + 'reconcile'
  else:
    # first page, might also be last for pass
    if int(last) > 1:
      return_url = referer + '?page=2'
    else:
      return_url = base + 'reconcile'
  # return to calling page
  return HttpResponseRedirect(return_url)

class PlacePortalView(DetailView):
  template_name = 'places/place_portal.html'

  #
  # given index id (whg_id) returned by typeahead/suggest, 
  # get its db record (a parent);
  # build array of place_ids (parent + children);
  # iterate those to build payload;
  # create add'l context values from set
  #

  def get_object(self):
    id_ = self.kwargs.get("id")
    print('args',self.args,'kwargs:',self.kwargs)
    es = settings.ES_CONN
    q = {"query":{"bool": {"must": [{"match":{"_id": id_}}]}}}
    pid=es.search(index='whg',body=q)['hits']['hits'][0]['_source']['place_id']
    # pid=es.search(index='whg', body=q)['hits']['hits'][0]['_source']['place_id']
    self.kwargs['pid'] = pid
    return get_object_or_404(Place, id=pid)

  def get_context_data(self, *args, **kwargs):
    print('get_context_data kwargs',self.kwargs)
    context = super(PlacePortalView, self).get_context_data(*args, **kwargs)
    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtokenwhg'] = settings.MAPBOX_TOKEN_WHG
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG
    es = settings.ES_CONN
    id_ = self.kwargs.get("id")
    pid = self.kwargs.get("pid")
    me = self.request.user
    place = get_object_or_404(Place, id=pid)
    if not me.is_anonymous:
      context['my_collections'] = Collection.objects.filter(owner=me, collection_class='place')
    context['whg_id'] = id_
    context['payload'] = [] # parent and children if any
    context['traces'] = [] # 
    context['allts'] = []
    # place portal headers gray for records from these
    context['core'] = ['gn500','gnmore','ne_countries','ne_rivers982','ne_mountains','wri_lakes','tgn_filtered_01']

    ids = [pid]
    # get child record ids from index
    q = {"query": {"parent_id": {"type": "child","id":id_}}}
    children = es.search(index='whg', body=q)['hits']
    for hit in children['hits']:
      #ids.append(int(hit['_id']))
      ids.append(int(hit['_source']['place_id']))

    # database records for parent + children into 'payload'
    qs=Place.objects.filter(id__in=ids).order_by('-whens__minmax')
    # TODO: better way of arriving at title
    context['title'] = qs.first().title

    collections = []
    annotations = []
    # qs is all attestations for a place in the index
    for place in qs:
      ds = Dataset.objects.get(id=place.dataset.id)
      # ds = get_object_or_404(Dataset,id=place.dataset.id)
      # temporally scoped attributes
      names = attribListFromSet('names',place.names.all())
      types = attribListFromSet('types',place.types.all())

      # collections, not traces 20220425
      # get traces, collections for this attestation
      attest_traces = list(place.traces.all())
      attest_collections = [t.collection for t in attest_traces if t.collection.status == "published"]
      # add to global list
      annotations = annotations + attest_traces
      collections = list(set(collections + attest_collections))

      geoms = [geom.jsonb for geom in place.geoms.all()]
      related = [rel.jsonb for rel in place.related.all()]
      
      # timespans generated upon Place record creation
      # draws from 'when' in names, types, geoms, relations
      # deliver to template in context
      timespans = list(t for t,_ in itertools.groupby(place.timespans)) if place.timespans else []
      context['allts'] += timespans
      
      record = {
        "whg_id":id_,
        "dataset":{"id":ds.id,"label": ds.label,
                   "name":ds.title,"webpage":ds.webpage},
        "place_id":place.id,
        "src_id":place.src_id, 
        "purl":ds.uri_base+str(place.id) if 'whgaz' in ds.uri_base else ds.uri_base+place.src_id,
        "title":place.title,
        "ccodes":place.ccodes, 
        "names":names, 
        "types":types, 
        "geoms":geoms,
        "related":related, 
        "links":[link.jsonb for link in place.links.distinct('jsonb') if not link.jsonb['identifier'].startswith('whg')], 
        "descriptions":[descr.jsonb for descr in place.descriptions.all()], 
        "depictions":[depict.jsonb for depict in place.depictions.all()],
        "minmax":place.minmax,
        "timespans":timespans
      }
      context['payload'].append(record)

    # collections & trace annotations from all attestations
    context['collections'] = collections
    context['annotations'] = annotations

    return context

class PlaceDetailView(DetailView):
  #login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'
  
  model = Place
  template_name = 'places/place_detail.html'

  
  def get_success_url(self):
    pid = self.kwargs.get("id")
    #user = self.request.user
    #print('messages:', messages.get_messages(self.kwargs))
    return '/places/'+str(pid)+'/detail'

  def get_object(self):
    pid = self.kwargs.get("id")
    return get_object_or_404(Place, id=pid)
  
  def get_context_data(self, *args, **kwargs):
    context = super(PlaceDetailView, self).get_context_data(*args, **kwargs)
    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG

    print('PlaceDetailView get_context_data() kwargs:',self.kwargs)
    print('PlaceDetailView get_context_data() request.user',self.request.user)
    place = get_object_or_404(Place, pk= self.kwargs.get("id"))
    ds = place.dataset
    me = self.request.user
    #placeset = Place.objects.filter(dataset=ds.label
    
    context['timespans'] = {'ts':place.timespans or None}
    context['minmax'] = {'mm':place.minmax or None}
    context['dataset'] = ds
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

    return context

# TODO:  tgn query very slow
class PlaceModalView(DetailView):
  model = Place

  template_name = 'places/place_modal.html'
  redirect_field_name = 'redirect_to'
    
  def get_success_url(self):
    pid = self.kwargs.get("id")
    #user = self.request.user
    return '/places/'+str(pid)+'/modal'

  def get_object(self):
    pid = self.kwargs.get("id")
    return get_object_or_404(Place, id=pid)
  
  def get_context_data(self, *args, **kwargs):
    context = super(PlaceModalView, self).get_context_data(*args, **kwargs)
    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG

    print('PlaceModalView get_context_data() kwargs:',self.kwargs)
    print('PlaceModalView get_context_data() request.user',self.request.user)
    place = get_object_or_404(Place, pk=self.kwargs.get("id"))
    ds = place.dataset
    dsobj = {"id":ds.id, "label":ds.label, "uri_base":ds.uri_base,
             "title":ds.title, "webpage":ds.webpage, 
             "minmax":None if ds.core else ds.minmax, 
             "creator":ds.creator, "last_modified":ds.last_modified_text} 
    #geomids = [geom.jsonb['properties']['id'] for geom in place.geoms.all()]
    #context['geoms'] = geoms
    context['dataset'] = dsobj
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

    return context

  # //
  # given place_id (pid), return abbreviated place_detail
  # //


class PlaceFullView(PlacePortalView):
  def render_to_response(self, context, **response_kwargs):
    return JsonResponse(context, **response_kwargs)

""" DEPRECATED """
# TODO: retire this trace implementation (replaced by collections)
# def mm_trace(tsarr):
#   if tsarr==[]:
#     return ''
#   else:
#     #print('mm_trace() tsarr',tsarr)
#     # TODO: not only simple years here; sorts string years?
#     starts = sorted( [t['start'] for t in tsarr] )
#     ends = sorted( [t['end'] for t in tsarr] )
#     mm = [min(starts), max(ends)]
#     mm = sorted(list(set([min(starts), max(ends)])))
#     return '('+str(mm[0])+('/'+str(mm[1]) if len(mm)>1 else '')+')'
#
# # get traces for this index parent and its children
# #print('ids',ids)
# qt = {"query": {"bool": {"must": [  {"terms":{"body.place_id": ids }}]}}}
# trace_hits = es.search(index='traces', doc_type='trace', body=qt)['hits']['hits']
# # for each hit, get target and aggregate body relation/when
# for h in trace_hits:
#   # filter bodies for place_id
#   bods=[b for b in h['_source']['body'] if b['place_id'] in ids]
#   bod = {
#     "id": bods[0]['id'],
#     "title": bods[0]['title'],
#     "place_id": bods[0]['place_id'],
#     "relations": [x['relations'][0]['relation'] +' '+mm_trace(x['relations'][0]['when']) for x in bods]
#   }
# context['traces'].append({
#   'trace_id':h['_id'],
#   'target':h['_source']['target'][0] if type(h['_source']['target']) == list else h['_source']['target'],
#   'body': bod,
#   'bodycount':len(h['_source']['body'])
# })



