# testy_whg.py 28 Feb 2021
# for Dataset id in list, perform whg recon, write summary to file

from django.contrib.auth.models import User, Group
from django.contrib.gis.geos import Polygon, Point, LineString
from django.shortcuts import get_object_or_404

import codecs
import simplejson as json
from datetime import date
from datasets.models import Dataset, Hit
from datasets.tasks import es_lookup_whg, normalize
from datasets.utils import *
#from datasets.views import ds_recon
from places.models import Place

someuser = get_object_or_404(User, pk=14)
whgadmin = get_object_or_404(User, pk=1)
today=date.today().strftime("%Y%m%d")
workdir = '/Users/karlg/Documents/Repos/_whgdata/elastic/whg/results/'
#dsids = 807
def whgpre(dsids):
  fout_summary = codecs.open(workdir + 'summary_' + str(dsids) + '.txt', mode='w', encoding = 'utf8')
  dsidlist = [int(x) for x in str(dsids).split(',')]
  idx='whg'
  for d in dsidlist:
    fout = codecs.open(workdir + 'whg_out_'+str(d)+'.txt', mode='w', encoding='utf8')
    datasets = Dataset.objects.filter(id__in=dsidlist).values_list('label')
    print('datasets', datasets)

    hit_parade = {"summary": {}, "hits": []}
    [count,count_hit,count_nohit,total_hits,count_p1,count_p2,count_p3] = [0,0,0,0,0,0,0]
    [nohits, some_hits, total_hits, count_nohits] = [[],0,0,0]
  
    qs = Place.objects.filter(dataset__in = datasets)
    bounds = {'type': ['userarea'], 'id': ['0']}
    #scope = 'all',
    #language = 'en'
    for place in qs:
      #place = qs[0]
      """
      build query object 'qobj'
      then result_obj = es_lookup_whg(qobj)
      """      
      count +=1
      qobj = {
        "place_id":place.id, 
        "src_id":place.src_id, 
        "title":place.title
      }
      [links,ccodes,types,variants,parents,geoms] = [[],[],[],[],[],[]]
  
      # links
      for l in place.links.all():
        links.append(l.jsonb['identifier'])
      qobj['links'] = links
  
      # ccodes (2-letter iso codes)
      for c in place.ccodes:
        ccodes.append(c)
      qobj['countries'] = list(set(place.ccodes))
  
      # types (Getty AAT identifiers)
      # accounts for 'null' in 97 black records
      for t in place.types.all():
        if t.jsonb['identifier'] != None:
          types.append(t.jsonb['identifier'])
        else:
          # no type? use inhabited place, cultural group, site
          types.extend(['aat:300008347','aat:300387171','aat:300000809'])
      qobj['placetypes'] = types
  
      # names
      for name in place.names.all():
        variants.append(name.toponym)
      qobj['variants'] = [v.lower() for v in variants]
  
      # parents
      for rel in place.related.all():
        if rel.jsonb['relationType'] == 'gvp:broaderPartitive':
          parents.append(rel.jsonb['label'])
      qobj['parents'] = parents
  
      # geoms
      if len(place.geoms.all()) > 0:
        # any geoms at all...
        g_list =[g.jsonb for g in place.geoms.all()]
        # make everything a simple polygon hull for spatial filter purposes
        qobj['geom'] = hully(g_list)
          
        
      # ***
      # run es_lookup_whg(qobj): 3 query passes
      # ***
      result_obj = es_lookup_whg(qobj, index=idx, bounds=bounds, place=place)
  
      if result_obj['hit_count'] == 0:
        count_nohit +=1
        nohits.append(result_obj['missed'])
      else:
        some_hits +=1
        for hit in result_obj['hits']:
          total_hits += 1
          hit_parade["hits"].append(hit)
  
    hits = hit_parade['hits']
    normalized_hits = []
    language = 'en'
    for h in hits:
      normalized_hits.append(normalize(h['_source'],'whg',language))
    fout.write('no hits:\n')
    for n in nohits:
      fout.write(str(n)+'\n')
    print(
      'pass0:'+str(len([h['_id'] for h in hits if h['pass'] == 'pass0']))+'; ',
      'pass1:'+str(len([h['_id'] for h in hits if h['pass'] == 'pass1']))+'; ',
      'pass2:'+str(len([h['_id'] for h in hits if h['pass'] == 'pass2'])),
    )
    fout.write('\n\nhits:\n'+json.dumps(hit_parade['hits'], indent=2))
    fout.write('\n\nnormalized hits:\n' + json.dumps(normalized_hits, indent=2))
    fout_summary.write('\ndsid '+str(d)+' -> some hits:'+str(some_hits)+'; total_hits: '+str(total_hits)+'; no hits: '+str(count_nohits))
    print('rows w/hits:'+str(some_hits)+'; total_hits: '+str(total_hits)+'; no hits: '+str(count_nohits))
    fout.close()
  fout_summary.close()
  
ds_array = input('one or more ds ids, comma delimited:   ')
whgpre(ds_array)

#done []

#delthese=[]
#for d in delthese:
  #ds=get_object_or_404(Dataset,pk=d)
  #ds.delete()