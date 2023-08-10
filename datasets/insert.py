# functions for inserting file data into database

import csv, json, re, sys
from django.contrib import messages
from django.contrib.gis.geos import GEOSGeometry
from django.db.utils import DataError
from django.http import HttpResponseServerError
from django.shortcuts import get_object_or_404, redirect

from .models import Dataset
from .utils import aliasIt, aat_lookup, ccodesFromGeom, \
  makeCoords, parse_wkt, parsedates_tsv, parsedates_lpf, emailer
from places.models import *

"""
  ds_insert_delim(df, pk)
  *** replaces ds_insert_tsv() ***
  insert delimited data into database
  if insert fails anywhere, delete dataset + any related objects
"""
# def ds_insert_delim(request, pk):
def ds_insert_delim(df, pk, user):
  csv.field_size_limit(300000)
  ds = get_object_or_404(Dataset, id=pk)
  # user = request.user
  print('ds_insert_tsv()',ds)
  # retrieve just-added file
  dsf = ds.files.all().order_by('-rev')[0]
  print('ds_insert_tsv(): ds, file', ds, dsf)
  # insert only if empty
  dbcount = Place.objects.filter(dataset = ds.label).count()
  # print('dbcount',dbcount)
  insert_errors = []
  if dbcount == 0:
    try:
      infile = dsf.file.open(mode="r")
      reader = csv.reader(infile, delimiter=dsf.delimiter)
      # reader = csv.reader(infile, delimiter='\t')

      infile.seek(0)
      header = next(reader, None)
      header = [col.lower().strip() for col in header]
      # print('header.lower()',[col.lower() for col in header])

      # strip BOM character if exists
      header[0] = header[0][1:] if '\ufeff' in header[0] else header[0]
      print('header', header)

      objs = {"PlaceName":[], "PlaceType":[], "PlaceGeom":[], "PlaceWhen":[],
              "PlaceLink":[], "PlaceRelated":[], "PlaceDescription":[]}

      # TODO: what if simultaneous inserts?
      countrows=0
      countlinked = 0
      total_links = 0
      for r in reader:
        # build attributes for new Place instance
        src_id = r[header.index('id')]
        title = r[header.index('title')].strip() # don't try to correct incoming except strip()
        title_source = r[header.index('title_source')]
        title_uri = r[header.index('title_uri')] if 'title_uri' in header else ''
        ccodes = r[header.index('ccodes')] if 'ccodes' in header else []
        variants = [x.strip() for x in r[header.index('variants')].split(';')] \
          if 'variants' in header and r[header.index('variants')] !='' else []
        types = [x.strip() for x in r[header.index('types')].split(';')] \
          if 'types' in header else []
        aat_types = [x.strip() for x in r[header.index('aat_types')].split(';')] \
          if 'aat_types' in header else []
        parent_name = r[header.index('parent_name')] if 'parent_name' in header else ''
        parent_id = r[header.index('parent_id')] if 'parent_id' in header else ''
        coords = makeCoords(r[header.index('lon')],r[header.index('lat')]) \
          if 'lon' in header and 'lat' in header else None
        geowkt = r[header.index('geowkt')] if 'geowkt' in header else None
        # replaced None with '' 25 May 2023
        geosource = r[header.index('geo_source')] if 'geo_source' in header else ''
        geoid = r[header.index('geo_id')] if 'geo_id' in header else None
        geojson = None # zero it out
        # make Point geometry from lon/lat if there
        if coords and len(coords) == 2:
          geojson = {"type": "Point", "coordinates": coords,
                      "geowkt": 'POINT('+str(coords[0])+' '+str(coords[1])+')'}
        # else make geometry (any) w/Shapely if geowkt
        if geowkt and geowkt not in ['']:
          geojson = parse_wkt(geowkt)
        if geojson and (geosource or geoid):
          geojson['citation']={'label':geosource,'id':geoid}

        # ccodes; compute if missing and there is geometry
        if len(ccodes) == 0:
          if geojson:
            try:
              ccodes = ccodesFromGeom(geojson)
            except:
              pass
          else:
            ccodes = []
        else:
          ccodes = [x.strip().upper() for x in r[header.index('ccodes')].split(';')]

        # TODO: assign aliases if wd, tgn, pl, bnf, gn, viaf
        matches = [aliasIt(x.strip()) for x in r[header.index('matches')].split(';')] \
          if 'matches' in header and r[header.index('matches')] != '' else []

        # TODO: patched Apr 2023; needs refactor
        # there _should_ always be a start or attestation_year
        # not forced by validation yet
        start = r[header.index('start')] if 'start' in header else None
        # source_year = r[header.index('attestation_year')] if 'attestation_year' in header else None
        has_end = 'end' in header and r[header.index('end')] !=''
        has_source_yr = 'attestation_year' in header and r[header.index('attestation_year')] !=''
        end = r[header.index('end')] if has_end else None
        source_year = r[header.index('attestation_year')] if has_source_yr else None
        dates = (start,end,source_year)
        print('dates tuple', dates)
        # must be start and/or source_year
        datesobj = parsedates_tsv(dates)
        # returns, e.g. {'timespans': [{'start': {'earliest': 1015}, 'end': None}],
        #  'minmax': [1015, None],
        #  'source_year': 1962}
        description = r[header.index('description')] \
          if 'description' in header else ''

        print('title, src_id (pre-newpl):', title, src_id)
        print('datesobj', datesobj)
        # create new Place object
        newpl = Place(
          src_id = src_id,
          dataset = ds,
          title = title,
          ccodes = ccodes,
          minmax = datesobj['minmax'],
          timespans = [datesobj['minmax']], # list of lists
          attestation_year = datesobj['source_year'] # integer or None
        )
        newpl.save()
        countrows += 1

        # build associated objects and add to arrays #
        #
        # PlaceName(); title, then variants
        #
        objs['PlaceName'].append(
          PlaceName(
            place=newpl,
            src_id = src_id,
            toponym = title,
            jsonb={"toponym": title, "citations": [{"id":title_uri,"label":title_source}]}
        ))
        # variants if any; assume same source as title toponym
        if len(variants) > 0:
          for v in variants:
            try:
              haslang = re.search("@(.*)$", v.strip())
              if len(v.strip()) > 200:
                # print(v.strip())
                pass
              else:
                # print('variant for', newpl.id, v)
                new_name = PlaceName(
                  place=newpl,
                  src_id = src_id,
                  toponym = v.strip(),
                  jsonb={"toponym": v.strip(), "citations": [{"id":"","label":title_source}]}
                )
                if haslang:
                  new_name.jsonb['lang'] = haslang.group(1)

                objs['PlaceName'].append(new_name)
            except:
              print('error on variant', sys.exc_info())
              print('error on variant for newpl.id', newpl.id, v)

        #
        # PlaceType()
        #
        if len(types) > 0:
          fclass_list=[]
          for i,t in enumerate(types):
            aatnum='aat:'+aat_types[i] if len(aat_types) >= len(types) and aat_types[i] !='' else None
            print('aatnum in insert_tsv() PlaceTypes', aatnum)
            if aatnum:
              try:
                fclass_list.append(get_object_or_404(Type, aat_id=int(aatnum[4:])).fclass)
              except:
                # report type lookup issue
                insert_errors.append(
                  {'src_id':src_id,
                  'title':newpl.title,
                  'field':'aat_type',
                  'msg':aatnum + ' not in WHG-supported list;'}
                )
                raise
            objs['PlaceType'].append(
              PlaceType(
                place=newpl,
                src_id = src_id,
                jsonb={ "identifier":aatnum if aatnum else '',
                        "sourceLabel":t,
                        "label":aat_lookup(int(aatnum[4:])) if aatnum else ''
                      }
            ))
          newpl.fclasses = fclass_list
          newpl.save()

        #
        # PlaceGeom()
        #
        print('geojson', geojson)
        if geojson:
          objs['PlaceGeom'].append(
            PlaceGeom(
              place=newpl,
              src_id = src_id,
              jsonb=geojson
              ,geom=GEOSGeometry(json.dumps(geojson))
          ))

        #
        # PlaceWhen()
        # via parsedates_tsv(): {"timespans":[{start{}, end{}}]}
        # if not start in ('',None):
        # if start != '':
        objs['PlaceWhen'].append(
          PlaceWhen(
            place=newpl,
            src_id = src_id,
            jsonb=datesobj
        ))

        #
        # PlaceLink() - all are closeMatch
        #
        if len(matches) > 0:
          countlinked += 1
          for m in matches:
            total_links += 1
            objs['PlaceLink'].append(
              PlaceLink(
                place=newpl,
                src_id = src_id,
                jsonb={"type":"closeMatch", "identifier":m}
            ))

        #
        # PlaceRelated()
        #
        if parent_name != '':
          objs['PlaceRelated'].append(
            PlaceRelated(
              place=newpl,
              src_id=src_id,
              jsonb={
                "relationType": "gvp:broaderPartitive",
                "relationTo": parent_id,
                "label": parent_name}
          ))

        #
        # PlaceDescription()
        # @id, value, lang
        if description != '':
          objs['PlaceDescription'].append(
            PlaceDescription(
              place=newpl,
              src_id = src_id,
              jsonb={
                #"@id": "", "value":description, "lang":""
                "value":description
              }
            ))


      # bulk_create(Class, batch_size=n) for each
      PlaceName.objects.bulk_create(objs['PlaceName'],batch_size=10000)
      PlaceType.objects.bulk_create(objs['PlaceType'],batch_size=10000)
      try:
        PlaceGeom.objects.bulk_create(objs['PlaceGeom'],batch_size=10000)
      except:
        print('geom insert failed', newpl, sys.exc_info())
        pass
      PlaceLink.objects.bulk_create(objs['PlaceLink'],batch_size=10000)
      PlaceRelated.objects.bulk_create(objs['PlaceRelated'],batch_size=10000)
      PlaceWhen.objects.bulk_create(objs['PlaceWhen'],batch_size=10000)
      PlaceDescription.objects.bulk_create(objs['PlaceDescription'],batch_size=10000)

      infile.close()
      print('insert_errors', insert_errors)
    except:
      print('tsv insert failed', newpl, sys.exc_info())
      # drop the (empty) dataset if insert wasn't complete
      ds.delete()
      # email to user, admin
      failed_upload_notification(user, dsf.file.name, ds.label)

      # return message to 500.html
      messages.error(request, "Database insert failed, but we don't know why. The WHG team has been notified and will follow up by email to <b>"+user.username+'</b> ('+user.email+')')
      return HttpResponseServerError()
  else:
    print('insert_tsv skipped, already in')
    messages.add_message(request, messages.INFO, 'data is uploaded, but problem displaying dataset page')
    return redirect('/mydata')

  return({"numrows":countrows,
          "numlinked":countlinked,
          "total_links":total_links})


""" 
  ds_insert_json(data, pk)
  *** replaces ds_insert_lpf() ***
  insert LPF into database
"""
# def ds_insert_json(request, pk):
def ds_insert_json(data, pk, user):
  import json
  [countrows,countlinked,total_links]= [0,0,0]

  if isinstance(data, str):
    jdata = json.loads(data)
  else:
    jdata = data

  ds = get_object_or_404(Dataset, id=pk)
  uribase = ds.uri_base
  dbcount = Place.objects.filter(dataset=ds.label).count()
  print(f"new dataset: {ds.label}, uri_base: {uribase}, data type: {type(jdata)}")

  # print('jdata', jdata)
  # TODO: lpf can get big; support json-lines

  if dbcount == 0:
    errors=[]
    print('count of features',len(jdata['features']))

    for feat in jdata['features']:
      # create Place, save to get id, then build associated records for each
      objs = {"PlaceNames":[], "PlaceTypes":[], "PlaceGeoms":[], "PlaceWhens":[],
              "PlaceLinks":[], "PlaceRelated":[], "PlaceDescriptions":[],
              "PlaceDepictions":[]}
      countrows += 1

      # build attributes for new Place instance
      title=re.sub('\(.*?\)', '', feat['properties']['title'])

      # geometry
      geojson = feat['geometry'] if 'geometry' in feat.keys() else None

      # ccodes
      if 'ccodes' not in feat['properties'].keys():
        if geojson:
          # a GeometryCollection
          ccodes = ccodesFromGeom(geojson)
          print('ccodes', ccodes)
        else:
          ccodes = []
      else:
        ccodes = feat['properties']['ccodes']

      # temporal
      # send entire feat for time summary
      # (minmax and intervals[])
      datesobj=parsedates_lpf(feat)

      # TODO: compute fclasses
      try:
        newpl = Place(
          # strip uribase from @id
          src_id=feat['@id'] if uribase in ['', None] else feat['@id'].replace(uribase,''),
          dataset=ds,
          title=title,
          ccodes=ccodes,
          minmax = datesobj['minmax'],
          timespans = datesobj['intervals']
        )
        newpl.save()
        print('new place: ',newpl.title)
      except:
        print('failed id' + title + 'datesobj: '+str(datesobj))
        print(sys.exc_info())

      # PlaceName: place,src_id,toponym,task_id,
      # jsonb:{toponym, lang, citation[{label, year, @id}], when{timespans, ...}}
      # TODO: adjust for 'ethnic', 'demonym'
      for n in feat['names']:
        if 'toponym' in n.keys():
          # if comma-separated listed, get first
          objs['PlaceNames'].append(PlaceName(
            place=newpl,
            src_id=newpl.src_id,
            toponym=n['toponym'].split(', ')[0],
            jsonb=n
          ))

      # PlaceType: place,src_id,task_id,jsonb:{identifier,label,src_label}
      #try:
      if 'types' in feat.keys():
        fclass_list = []
        for t in feat['types']:
          if 'identifier' in t.keys() and t['identifier'][:4] == 'aat:' \
             and int(t['identifier'][4:]) in Type.objects.values_list('aat_id',flat=True):
            fc = get_object_or_404(Type, aat_id=int(t['identifier'][4:])).fclass \
              if t['identifier'][:4] == 'aat:' else None
            fclass_list.append(fc)
          else:
            fc = None
          print('from feat[types]:',t)
          print('PlaceType record newpl,newpl.src_id,t,fc',newpl,newpl.src_id,t,fc)
          objs['PlaceTypes'].append(PlaceType(
            place=newpl,
            src_id=newpl.src_id,
            jsonb=t,
            fclass=fc
          ))
        newpl.fclasses = fclass_list
        newpl.save()

      # PlaceWhen: place,src_id,task_id,minmax,jsonb:{timespans[],periods[],label,duration}
      if 'when' in feat.keys() and feat['when'] != {}:
        objs['PlaceWhens'].append(PlaceWhen(
          place=newpl,
          src_id=newpl.src_id,
          jsonb=feat['when'],
          minmax=newpl.minmax
        ))

      # PlaceGeom: place,src_id,task_id,jsonb:{type,coordinates[],when{},geo_wkt,src}
      #if 'geometry' in feat.keys() and feat['geometry']['type']=='GeometryCollection':
      if geojson and geojson['type']=='GeometryCollection':
        #for g in feat['geometry']['geometries']:
        for g in geojson['geometries']:
          # print('from feat[geometry]:',g)
          objs['PlaceGeoms'].append(PlaceGeom(
            place=newpl,
            src_id=newpl.src_id,
            jsonb=g
            ,geom=GEOSGeometry(json.dumps(g))
          ))
      elif geojson:
        objs['PlaceGeoms'].append(PlaceGeom(
          place=newpl,
          src_id=newpl.src_id,
          jsonb=geojson
          ,geom=GEOSGeometry(json.dumps(geojson))
        ))

      # PlaceLink: place,src_id,task_id,jsonb:{type,identifier}
      if 'links' in feat.keys() and len(feat['links'])>0:
        countlinked +=1 # record has *any* links
        #print('countlinked',countlinked)
        for l in feat['links']:
          total_links += 1 # record has n links
          objs['PlaceLinks'].append(PlaceLink(
            place=newpl,
            src_id=newpl.src_id,
            # alias uri base for known authorities
            jsonb={"type":l['type'], "identifier": aliasIt(l['identifier'].rstrip('/'))}
          ))

      # PlaceRelated: place,src_id,task_id,jsonb{relationType,relationTo,label,when{}}
      if 'relations' in feat.keys():
        for r in feat['relations']:
          objs['PlaceRelated'].append(PlaceRelated(
            place=newpl,src_id=newpl.src_id,jsonb=r))

      # PlaceDescription: place,src_id,task_id,jsonb{@id,value,lang}
      if 'descriptions' in feat.keys():
        for des in feat['descriptions']:
          objs['PlaceDescriptions'].append(PlaceDescription(
            place=newpl,src_id=newpl.src_id,jsonb=des))

      # PlaceDepiction: place,src_id,task_id,jsonb{@id,title,license}
      if 'depictions' in feat.keys():
        for dep in feat['depictions']:
          objs['PlaceDepictions'].append(PlaceDepiction(
            place=newpl,src_id=newpl.src_id,jsonb=dep))

      # throw errors into user message
      def raiser(model, e):
        print('Bulk load for '+ model + ' failed on', newpl)
        errors.append({"field":model, "error":e})
        print('error', e)
        raise DataError

      # create related objects
      try:
        PlaceName.objects.bulk_create(objs['PlaceNames'])
      except DataError as e:
        raiser('Name', e)

      try:
        PlaceType.objects.bulk_create(objs['PlaceTypes'])
      except DataError as de:
        raiser('Type', e)

      try:
        PlaceWhen.objects.bulk_create(objs['PlaceWhens'])
      except DataError as de:
        raiser('When', e)

      try:
        PlaceGeom.objects.bulk_create(objs['PlaceGeoms'])
      except DataError as de:
        raiser('Geom', e)

      try:
        PlaceLink.objects.bulk_create(objs['PlaceLinks'])
      except DataError as de:
        raiser('Link', e)

      try:
        PlaceRelated.objects.bulk_create(objs['PlaceRelated'])
      except DataError as de:
        raiser('Related', e)

      try:
        PlaceDescription.objects.bulk_create(objs['PlaceDescriptions'])
      except DataError as de:
        raiser('Description', e)

      try:
        PlaceDepiction.objects.bulk_create(objs['PlaceDepictions'])
      except DataError as de:
        raiser('Depiction', e)

      # TODO: compute newpl.ccodes (if geom), newpl.fclasses, newpl.minmax
      # something failed in *any* Place creation; delete dataset

    print('new dataset:', ds.__dict__)

    return({"numrows":countrows,
            "numlinked":countlinked,
            "total_links":total_links})
  else:
    print('insert_ skipped, data already in')
    return redirect('/mydata')

def failed_upload_notification(user, fn, ds=None):
  subj = 'World Historical Gazetteer error followup '
  subj += 'on dataset (' + ds + ')' if ds else ''
  msg = 'Hello ' + user.username + \
        ', \n\nWe see your recent upload was not successful '
  if ds:
    msg += 'on insert to the database '
  else:
    msg += 'on initial validation '
  msg += '-- very sorry about that! ' + \
         '\nWe will look into why and get back to you within a day.\n\nRegards,\nThe WHG Team\n\n\n[' + fn + ']'
  emailer(subj, msg, settings.DEFAULT_FROM_EMAIL,
          [user.email, settings.EMAIL_HOST_USER])


""" 
	DEPRECATED 2023-08
  ds_insert_lpf
  insert LPF into database
"""
def ds_insert_lpf(request, pk):
  import json
  [countrows,countlinked,total_links]= [0,0,0]
  ds = get_object_or_404(Dataset, id=pk)
  user = request.user
  # latest file
  dsf = ds.files.all().order_by('-rev')[0]
  uribase = ds.uri_base
  print('new dataset, uri_base', ds.label, uribase)

  # TODO: lpf can get big; support json-lines

  # insert only if empty
  dbcount = Place.objects.filter(dataset = ds.label).count()
  print('dbcount',dbcount)

  if dbcount == 0:
    errors=[]
    try:
      infile = dsf.file.open(mode="r")
      print('ds_insert_lpf() for dataset',ds)
      print('ds_insert_lpf() request.GET, infile',request.GET,infile)
      with infile:
        jdata = json.loads(infile.read())

        print('count of features',len(jdata['features']))
        #print('0th feature',jdata['features'][0])

        for feat in jdata['features']:
          # create Place, save to get id, then build associated records for each
          objs = {"PlaceNames":[], "PlaceTypes":[], "PlaceGeoms":[], "PlaceWhens":[],
                  "PlaceLinks":[], "PlaceRelated":[], "PlaceDescriptions":[],
                  "PlaceDepictions":[]}
          countrows += 1

          # build attributes for new Place instance
          title=re.sub('\(.*?\)', '', feat['properties']['title'])

          # geometry
          geojson = feat['geometry'] if 'geometry' in feat.keys() else None

          # ccodes
          if 'ccodes' not in feat['properties'].keys():
            if geojson:
              # a GeometryCollection
              ccodes = ccodesFromGeom(geojson)
              print('ccodes', ccodes)
            else:
              ccodes = []
          else:
            ccodes = feat['properties']['ccodes']

          # temporal
          # send entire feat for time summary
          # (minmax and intervals[])
          datesobj=parsedates_lpf(feat)

          # TODO: compute fclasses
          try:
            newpl = Place(
              # strip uribase from @id
              src_id=feat['@id'] if uribase in ['', None] else feat['@id'].replace(uribase,''),
              dataset=ds,
              title=title,
              ccodes=ccodes,
              minmax = datesobj['minmax'],
              timespans = datesobj['intervals']
            )
            newpl.save()
            print('new place: ',newpl.title)
          except:
            print('failed id' + title + 'datesobj: '+str(datesobj))
            print(sys.exc_info())

          # PlaceName: place,src_id,toponym,task_id,
          # jsonb:{toponym, lang, citation[{label, year, @id}], when{timespans, ...}}
          # TODO: adjust for 'ethnic', 'demonym'
          for n in feat['names']:
            if 'toponym' in n.keys():
              # if comma-separated listed, get first
              objs['PlaceNames'].append(PlaceName(
                place=newpl,
                src_id=newpl.src_id,
                toponym=n['toponym'].split(', ')[0],
                jsonb=n
              ))

          # PlaceType: place,src_id,task_id,jsonb:{identifier,label,src_label}
          #try:
          if 'types' in feat.keys():
            fclass_list = []
            for t in feat['types']:
              if 'identifier' in t.keys() and t['identifier'][:4] == 'aat:' \
                 and int(t['identifier'][4:]) in Type.objects.values_list('aat_id',flat=True):
                fc = get_object_or_404(Type, aat_id=int(t['identifier'][4:])).fclass \
                  if t['identifier'][:4] == 'aat:' else None
                fclass_list.append(fc)
              else:
                fc = None
              print('from feat[types]:',t)
              print('PlaceType record newpl,newpl.src_id,t,fc',newpl,newpl.src_id,t,fc)
              objs['PlaceTypes'].append(PlaceType(
                place=newpl,
                src_id=newpl.src_id,
                jsonb=t,
                fclass=fc
              ))
            newpl.fclasses = fclass_list
            newpl.save()

          # PlaceWhen: place,src_id,task_id,minmax,jsonb:{timespans[],periods[],label,duration}
          if 'when' in feat.keys() and feat['when'] != {}:
            objs['PlaceWhens'].append(PlaceWhen(
              place=newpl,
              src_id=newpl.src_id,
              jsonb=feat['when'],
              minmax=newpl.minmax
            ))

          # PlaceGeom: place,src_id,task_id,jsonb:{type,coordinates[],when{},geo_wkt,src}
          #if 'geometry' in feat.keys() and feat['geometry']['type']=='GeometryCollection':
          if geojson and geojson['type']=='GeometryCollection':
            #for g in feat['geometry']['geometries']:
            for g in geojson['geometries']:
              # print('from feat[geometry]:',g)
              objs['PlaceGeoms'].append(PlaceGeom(
                place=newpl,
                src_id=newpl.src_id,
                jsonb=g
                ,geom=GEOSGeometry(json.dumps(g))
              ))
          elif geojson:
            objs['PlaceGeoms'].append(PlaceGeom(
              place=newpl,
              src_id=newpl.src_id,
              jsonb=geojson
              ,geom=GEOSGeometry(json.dumps(geojson))
            ))

          # PlaceLink: place,src_id,task_id,jsonb:{type,identifier}
          if 'links' in feat.keys() and len(feat['links'])>0:
            countlinked +=1 # record has *any* links
            #print('countlinked',countlinked)
            for l in feat['links']:
              total_links += 1 # record has n links
              objs['PlaceLinks'].append(PlaceLink(
                place=newpl,
                src_id=newpl.src_id,
                # alias uri base for known authorities
                jsonb={"type":l['type'], "identifier": aliasIt(l['identifier'].rstrip('/'))}
              ))

          # PlaceRelated: place,src_id,task_id,jsonb{relationType,relationTo,label,when{}}
          if 'relations' in feat.keys():
            for r in feat['relations']:
              objs['PlaceRelated'].append(PlaceRelated(
                place=newpl,src_id=newpl.src_id,jsonb=r))

          # PlaceDescription: place,src_id,task_id,jsonb{@id,value,lang}
          if 'descriptions' in feat.keys():
            for des in feat['descriptions']:
              objs['PlaceDescriptions'].append(PlaceDescription(
                place=newpl,src_id=newpl.src_id,jsonb=des))

          # PlaceDepiction: place,src_id,task_id,jsonb{@id,title,license}
          if 'depictions' in feat.keys():
            for dep in feat['depictions']:
              objs['PlaceDepictions'].append(PlaceDepiction(
                place=newpl,src_id=newpl.src_id,jsonb=dep))

          # throw errors into user message
          def raiser(model, e):
            print('Bulk load for '+ model + ' failed on', newpl)
            errors.append({"field":model, "error":e})
            print('error', e)
            raise DataError

          # create related objects
          try:
            PlaceName.objects.bulk_create(objs['PlaceNames'])
          except DataError as e:
            raiser('Name', e)

          try:
            PlaceType.objects.bulk_create(objs['PlaceTypes'])
          except DataError as de:
            raiser('Type', e)

          try:
            PlaceWhen.objects.bulk_create(objs['PlaceWhens'])
          except DataError as de:
            raiser('When', e)

          try:
            PlaceGeom.objects.bulk_create(objs['PlaceGeoms'])
          except DataError as de:
            raiser('Geom', e)

          try:
            PlaceLink.objects.bulk_create(objs['PlaceLinks'])
          except DataError as de:
            raiser('Link', e)

          try:
            PlaceRelated.objects.bulk_create(objs['PlaceRelated'])
          except DataError as de:
            raiser('Related', e)

          try:
            PlaceDescription.objects.bulk_create(objs['PlaceDescriptions'])
          except DataError as de:
            raiser('Description', e)

          try:
            PlaceDepiction.objects.bulk_create(objs['PlaceDepictions'])
          except DataError as de:
            raiser('Depiction', e)

          # TODO: compute newpl.ccodes (if geom), newpl.fclasses, newpl.minmax
          # something failed in *any* Place creation; delete dataset

        print('new dataset:', ds.__dict__)
        infile.close()

      return({"numrows":countrows,
              "numlinked":countlinked,
              "total_links":total_links})
    except:
      # drop the (empty) database
      # ds.delete()
      # email to user, admin
      subj = 'World Historical Gazetteer error followup'
      msg = 'Hello '+ user.username+', \n\nWe see your recent upload for the '+ds.label+\
            ' dataset failed, very sorry about that!'+\
            '\nThe likely cause was: '+str(errors)+'\n\n'+\
            "If you can, fix the cause. If not, please respond to this email and we will get back to you soon.\n\nRegards,\nThe WHG Team"
      emailer(subj,msg,'whg@kgeographer.org',[user.email, 'whgadmin@kgeographer.com'])

      # return message to 500.html
      # messages.error(request, "Database insert failed, but we don't know why. The WHG team has been notified and will follow up by email to <b>"+user.username+'</b> ('+user.email+')')
      # return redirect(request.GET.get('from'))
      return HttpResponseServerError()

  else:
    print('insert_ skipped, already in')
    messages.add_message(request, messages.INFO, 'data is uploaded, but problem displaying dataset page')
    return redirect('/mydata')

"""
	DEPRECATED 2023-08
  ds_insert_tsv(pk)
  insert tsv into database
  file is validated, dataset exists
  if insert fails anywhere, delete dataset + any related objects
"""
def ds_insert_tsv(request, pk):
  import csv, re
  csv.field_size_limit(300000)
  ds = get_object_or_404(Dataset, id=pk)
  user = request.user
  print('ds_insert_tsv()',ds)
  # retrieve just-added file
  dsf = ds.files.all().order_by('-rev')[0]
  print('ds_insert_tsv(): ds, file', ds, dsf)
  # insert only if empty
  dbcount = Place.objects.filter(dataset = ds.label).count()
  # print('dbcount',dbcount)
  insert_errors = []
  if dbcount == 0:
    try:
      infile = dsf.file.open(mode="r")
      reader = csv.reader(infile, delimiter=dsf.delimiter)
      # reader = csv.reader(infile, delimiter='\t')

      infile.seek(0)
      header = next(reader, None)
      header = [col.lower().strip() for col in header]
      # print('header.lower()',[col.lower() for col in header])

      # strip BOM character if exists
      header[0] = header[0][1:] if '\ufeff' in header[0] else header[0]
      print('header', header)

      objs = {"PlaceName":[], "PlaceType":[], "PlaceGeom":[], "PlaceWhen":[],
              "PlaceLink":[], "PlaceRelated":[], "PlaceDescription":[]}

      # TODO: what if simultaneous inserts?
      countrows=0
      countlinked = 0
      total_links = 0
      for r in reader:
        # build attributes for new Place instance
        src_id = r[header.index('id')]
        title = r[header.index('title')].strip() # don't try to correct incoming except strip()
        # title = r[header.index('title')].replace("' ","'") # why?
        # strip anything in parens for title only
        # title = re.sub('\(.*?\)', '', title)
        title_source = r[header.index('title_source')]
        title_uri = r[header.index('title_uri')] if 'title_uri' in header else ''
        ccodes = r[header.index('ccodes')] if 'ccodes' in header else []
        variants = [x.strip() for x in r[header.index('variants')].split(';')] \
          if 'variants' in header and r[header.index('variants')] !='' else []
        types = [x.strip() for x in r[header.index('types')].split(';')] \
          if 'types' in header else []
        aat_types = [x.strip() for x in r[header.index('aat_types')].split(';')] \
          if 'aat_types' in header else []
        parent_name = r[header.index('parent_name')] if 'parent_name' in header else ''
        parent_id = r[header.index('parent_id')] if 'parent_id' in header else ''
        coords = makeCoords(r[header.index('lon')],r[header.index('lat')]) \
          if 'lon' in header and 'lat' in header else None
        geowkt = r[header.index('geowkt')] if 'geowkt' in header else None
        # replaced None with '' 25 May 2023
        geosource = r[header.index('geo_source')] if 'geo_source' in header else ''
        geoid = r[header.index('geo_id')] if 'geo_id' in header else None
        geojson = None # zero it out
        # print('geosource:', geosource)
        # print('geoid:', geoid)
        # make Point geometry from lon/lat if there
        if coords and len(coords) == 2:
          geojson = {"type": "Point", "coordinates": coords,
                      "geowkt": 'POINT('+str(coords[0])+' '+str(coords[1])+')'}
        # else make geometry (any) w/Shapely if geowkt
        if geowkt and geowkt not in ['']:
          geojson = parse_wkt(geowkt)
        if geojson and (geosource or geoid):
          geojson['citation']={'label':geosource,'id':geoid}

        # ccodes; compute if missing and there is geometry
        if len(ccodes) == 0:
          if geojson:
            try:
              ccodes = ccodesFromGeom(geojson)
            except:
              pass
          else:
            ccodes = []
        else:
          ccodes = [x.strip().upper() for x in r[header.index('ccodes')].split(';')]

        # TODO: assign aliases if wd, tgn, pl, bnf, gn, viaf
        matches = [aliasIt(x.strip()) for x in r[header.index('matches')].split(';')] \
          if 'matches' in header and r[header.index('matches')] != '' else []

        # TODO: patched Apr 2023; needs refactor
        # there _should_ always be a start or attestation_year
        # not forced by validation yet
        # start = r[header.index('start')] if 'start' in header else None
        start = r[header.index('start')] if 'start' in header else None
        # source_year = r[header.index('attestation_year')] if 'attestation_year' in header else None
        has_end = 'end' in header and r[header.index('end')] !=''
        has_source_yr = 'attestation_year' in header and r[header.index('attestation_year')] !=''
        end = r[header.index('end')] if has_end else None
        source_year = r[header.index('attestation_year')] if has_source_yr else None
        # end = r[header.index('end')] if has_end else start
        # print('row r' , r)
        # print('start:'+start,'; end:'+end, ';year'+source_year)
        dates = (start,end,source_year)
        print('dates tuple', dates)
        # must be start and/or source_year
        datesobj = parsedates_tsv(dates)
        # returns, e.g. {'timespans': [{'start': {'earliest': 1015}, 'end': None}],
        #  'minmax': [1015, None],
        #  'source_year': 1962}
        description = r[header.index('description')] \
          if 'description' in header else ''

        print('title, src_id (pre-newpl):', title, src_id)
        print('datesobj', datesobj)
        # create new Place object
        newpl = Place(
          src_id = src_id,
          dataset = ds,
          title = title,
          ccodes = ccodes,
          minmax = datesobj['minmax'],
          timespans = [datesobj['minmax']], # list of lists
          attestation_year = datesobj['source_year'] # integer or None
        )
        newpl.save()
        countrows += 1

        # build associated objects and add to arrays #
        #
        # PlaceName(); title, then variants
        #
        objs['PlaceName'].append(
          PlaceName(
            place=newpl,
            src_id = src_id,
            toponym = title,
            jsonb={"toponym": title, "citations": [{"id":title_uri,"label":title_source}]}
        ))
        # variants if any; assume same source as title toponym
        if len(variants) > 0:
          for v in variants:
            try:
              haslang = re.search("@(.*)$", v.strip())
              if len(v.strip()) > 200:
                # print(v.strip())
                pass
              else:
                # print('variant for', newpl.id, v)
                new_name = PlaceName(
                  place=newpl,
                  src_id = src_id,
                  toponym = v.strip(),
                  jsonb={"toponym": v.strip(), "citations": [{"id":"","label":title_source}]}
                )
                if haslang:
                  new_name.jsonb['lang'] = haslang.group(1)

                objs['PlaceName'].append(new_name)
            except:
              print('error on variant', sys.exc_info())
              print('error on variant for newpl.id', newpl.id, v)

        #
        # PlaceType()
        #
        if len(types) > 0:
          fclass_list=[]
          for i,t in enumerate(types):
            aatnum='aat:'+aat_types[i] if len(aat_types) >= len(types) and aat_types[i] !='' else None
            print('aatnum in insert_tsv() PlaceTypes', aatnum)
            if aatnum:
              try:
                fclass_list.append(get_object_or_404(Type, aat_id=int(aatnum[4:])).fclass)
              except:
                # report type lookup issue
                insert_errors.append(
                  {'src_id':src_id,
                  'title':newpl.title,
                  'field':'aat_type',
                  'msg':aatnum + ' not in WHG-supported list;'}
                )
                raise
                # messages.add_message(request, messages.INFO, 'choked on an invalid AAT place type id')
                # return redirect('/mydata')
                # continue
            objs['PlaceType'].append(
              PlaceType(
                place=newpl,
                src_id = src_id,
                jsonb={ "identifier":aatnum if aatnum else '',
                        "sourceLabel":t,
                        "label":aat_lookup(int(aatnum[4:])) if aatnum else ''
                      }
            ))
          newpl.fclasses = fclass_list
          newpl.save()

        #
        # PlaceGeom()
        #
        print('geojson', geojson)
        if geojson:
          objs['PlaceGeom'].append(
            PlaceGeom(
              place=newpl,
              src_id = src_id,
              jsonb=geojson
              ,geom=GEOSGeometry(json.dumps(geojson))
          ))

        #
        # PlaceWhen()
        # via parsedates_tsv(): {"timespans":[{start{}, end{}}]}
        # if not start in ('',None):
        # if start != '':
        objs['PlaceWhen'].append(
          PlaceWhen(
            place=newpl,
            src_id = src_id,
            #jsonb=datesobj['timespans']
            jsonb=datesobj
        ))

        #
        # PlaceLink() - all are closeMatch
        #
        if len(matches) > 0:
          countlinked += 1
          for m in matches:
            total_links += 1
            objs['PlaceLink'].append(
              PlaceLink(
                place=newpl,
                src_id = src_id,
                jsonb={"type":"closeMatch", "identifier":m}
            ))

        #
        # PlaceRelated()
        #
        if parent_name != '':
          objs['PlaceRelated'].append(
            PlaceRelated(
              place=newpl,
              src_id=src_id,
              jsonb={
                "relationType": "gvp:broaderPartitive",
                "relationTo": parent_id,
                "label": parent_name}
          ))

        #
        # PlaceDescription()
        # @id, value, lang
        if description != '':
          objs['PlaceDescription'].append(
            PlaceDescription(
              place=newpl,
              src_id = src_id,
              jsonb={
                #"@id": "", "value":description, "lang":""
                "value":description
              }
            ))


      # bulk_create(Class, batch_size=n) for each
      PlaceName.objects.bulk_create(objs['PlaceName'],batch_size=10000)
      PlaceType.objects.bulk_create(objs['PlaceType'],batch_size=10000)
      try:
        PlaceGeom.objects.bulk_create(objs['PlaceGeom'],batch_size=10000)
      except:
        print('geom insert failed', newpl, sys.exc_info())
        pass
      PlaceLink.objects.bulk_create(objs['PlaceLink'],batch_size=10000)
      PlaceRelated.objects.bulk_create(objs['PlaceRelated'],batch_size=10000)
      PlaceWhen.objects.bulk_create(objs['PlaceWhen'],batch_size=10000)
      PlaceDescription.objects.bulk_create(objs['PlaceDescription'],batch_size=10000)

      infile.close()
      print('insert_errors', insert_errors)
      # print('rows,linked,links:', countrows, countlinked, total_links)
    except:
      print('tsv insert failed', newpl, sys.exc_info())
      # drop the (empty) dataset if insert wasn't complete
      ds.delete()
      # email to user, admin
      failed_upload_notification(user, dsf.file.name, ds.label)

      # return message to 500.html
      messages.error(request, "Database insert failed, but we don't know why. The WHG team has been notified and will follow up by email to <b>"+user.username+'</b> ('+user.email+')')
      return HttpResponseServerError()
  else:
    print('insert_tsv skipped, already in')
    messages.add_message(request, messages.INFO, 'data is uploaded, but problem displaying dataset page')
    return redirect('/mydata')

  return({"numrows":countrows,
          "numlinked":countlinked,
          "total_links":total_links})

