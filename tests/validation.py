# validation experiments
# csv: goodtables
# LPF: jsonschema

import os, sys, json, pprint, codecs
from goodtables import validate
pp = pprint.PrettyPrinter(indent=1)

dslabel = 'diamonds135'
schema=codecs.open('tests/whg/datapackage.json','r','utf-8')
dp = json.loads(schema.read())
dp['resources'][0]['name'] = dslabel
dp['resources'][0]['path'] = "tests/whg/"+dslabel+".tsv"
#report = validate('tests/invalid.csv')
report = validate(dp)
pp.pprint(report)
pp.pprint(report['tables'][0]['errors'])

#report['valid'] # false
#report['table-count'] # 1
#report['error-count'] # 3
#report['tables'][0]['valid'] # false
#report['tables'][0]['source'] # 'invalid.csv'
#report['tables'][0]['errors'][0]['code'] # 'blank-header'
