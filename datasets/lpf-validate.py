import codecs, tempfile, os, re, ipdb, sys,pout,time, datetime, json,sys
import simplejson as json
from jsonschema import validate, Draft7Validator, draft7_format_checker
#flines = 'ToposTextGazetteer.jsonl'

os.chdir('/Users/karlg/Documents/Repos/_whgdata/')
schema = json.loads(codecs.open('../_whg/datasets/static/lpf-schema-jsonl.json', 'r', 'utf8').read())
fcoll = 'data/_source/ToposText/ToposTextGazetteer.jsonld'
fincoll = codecs.open(fcoll, 'r', 'utf8')
jdata = json.loads(fincoll.read())
fout = codecs.open('validate-lpf-result.txt', 'w', 'utf8')

result = {"errors":[],"format":"lpf"}
[countrows,count_ok] = [0,0]

if ['type', '@context', 'features'] != list(jdata.keys()) \
   or jdata['type'] != 'FeatureCollection' \
   or len(jdata['features']) == 0:
  print('not valid GeoJSON-LD')
else:
  for feat in jdata['features'][:3]:
    countrows +=1
    print(feat['properties']['title'])
    try:
      validate(
        instance=feat,
        schema=schema,
        format_checker=draft7_format_checker
      )
      count_ok +=1
    except:
      err = sys.exc_info()
      print('some kinda error',err)
      result["errors"].append({"feat":countrows-1,'error':err[1].args[0]})

fout.write(json.dumps(result["errors"]))
fout.close()
result['count'] = countrows
#return result