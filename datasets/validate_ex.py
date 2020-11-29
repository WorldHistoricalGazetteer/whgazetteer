# validate.py
# using frictionless

from frictionless import validate_table, describe, extract
from pprint import pprint as pp
data = '/Users/karlg/Documents/Repos/_whgazetteer/_testdata/validate/data_ex2.tsv'
#data = '{/some/dir/}data_ex.tsv'

# required fields: "id","title", "title_source"

def v(data):
    global descrip, rows, report, req
    report=validate_table(data, schema=sch, 
                        skip_errors=['missing-cell'],
                        sync_schema=True)
    descrip = describe(data)
    rows = extract(data)
    
    return pp(report)

sch = {"fields": [
  {"name": "id",
    "unique": True,
    "description": "contributor's permanent identifier",
    "constraints": {
      "required": True
    }
  },
  {"name": "title",
    "type": "string",
    "description": "A single 'preferred' place name, serving as title for the record",
    "constraints": {
      "required": True
    }
  },
  {"name": "title_source",
    "type": "string",
    "description": "String label for source (not URI)",
    "constraints": {
      "required": True
    }
  },
  {"name": "ccodes",
    "type": "string",
    "description": "One or more ISO Alpha-2 country codes, delimited with ';'",
    "constraints": {
     "pattern": "([a-zA-Z]{2};?)+" 
    }
  }
],
  "primaryKey": "id",
  "missingValues": [""," ","null","Null","None"]
}

v(data)