from django.shortcuts import get_object_or_404

from places.models import Place, PlaceLink
from datasets.models import Dataset, Hit
import re, sys


tid='00dee205-7d2f-4b15-b067-7efc61a063ff'
dslabel = 'gnmore'
#tid='d6ad4289-cae6-476d-873c-a81fed4d6315'
#dslabel = 'black'

# get unreviewed hits
hitplaces = Hit.objects.values('place_id').filter(
  task_id=tid,reviewed=False)
# initialize
ds=get_object_or_404(Dataset, label=dslabel)
qs = Place.objects.all().filter(id__in=hitplaces).order_by('title')
count, count_matched, count_links = [0,0,0]

for p in qs:
    count +=1
    #p = get_object_or_404(Place, id= 2487488)
    pid = p.id
    plinks=[l.jsonb['identifier'] for l in p.links.all()]
    #print(pid,plinks)
    hits = Hit.objects.all().filter(task_id = tid, place_id_id = pid)
    objs = {"PlaceLinks":[]}
    for h in hits:
        try:
            hitmatches = [re.search('^.*?: (.*)$',l).group(1) for l in h.json['links']] if h.json['links'] else []
        except:
            pass
            #print('quit on place ',pid,h.json)
            #sys.exit()
        shared = len(set(hitmatches) & set(plinks)) > 0 #and 'wd:'+h.authrecord_id not in plinks # t/f
        noQ = 'wd:'+h.authrecord_id not in plinks
        # if they share a link, write a place_link record for the authrecord_id 
        # and any hitmatches not in plinks
        if shared:
            count_matched +=1
            print('shared link(s):',list(set(hitmatches) & set(plinks)))
            add_these = list(set(hitmatches) - set(plinks))
            #add_these = set(hitmatches).difference(plinks)
            if noQ:
                add_these.append('wd:'+h.authrecord_id)
            count_links += len(add_these)
            print('adding place_link records for '+str(pid)+':', add_these)
            
            # write place_link records; set hit record 'reviewed' flag
            if len(add_these) > 0:
                for linkid in add_these:
                    objs['PlaceLinks'].append(PlaceLink(
                        place_id=p,src_id=p.src_id,task_id=tid,
                        jsonb={"type": 'closeMatch',"identifier": linkid})
                    )
                    #print(linkid,obj)
                
                # flag the hits in this set
            for h in hits:
                matchee = get_object_or_404(Hit, id=h.id)
                matchee.reviewed = True
                matchee.save()
        else:
            print('nothing for '+p.title+'('+str(pid)+')')
    PlaceLink.objects.bulk_create(objs['PlaceLinks'])
    ds.numlinked += count_matched
    ds.total_links += count_links
    ds.save()    
print(str(count_matched)+' of '+str(count)+' shared a match, so...')
print(str(count_links)+' place_link records were written')

#link = PlaceLink.objects.create(
  #place_id = pid,
  #task_id = tid,
  #src_id = p.src_id,
  #jsonb = {
    #"type": 'closeMatch',
    #"identifier": linkid
  #}
#)
# update totals
#ds.numlinked = ds.numlinked +1
#ds.total_links = ds.total_links +1
#ds.save()
