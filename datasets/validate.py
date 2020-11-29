# validate.py
# using frictionless

from goodtables import validate as gvalidate
from frictionless import extract, validate, describe, validate_schema, validate_table
from pprint import pprint as pp
import codecs, json, mimetypes
wd = '/Users/karlg/repos/_whgazetteer/_testdata/validate/'
#sch_new = wd+'schema_ex.json'
sch_new = wd+'schema_csv.json'
sch = wd + 'schema_tsv.json'
#tempfn = wd+files[0]

# mimetypes.guess_type(fn, strict=True)
# text/csv
# text/tab-separated-values
# application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
# application/vnd.oasis.opendocument.spreadsheet
mimes = ['text/csv','text/tab-separated-values','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',' application/vnd.oasis.opendocument.spreadsheet','application/json']
files = ['croniken20.tsv','bdda34.csv','bdda34_err1.csv','bdda34_err1.tsv','bdda34.xlsx','bdda34.ods','lugares60.json']
# 0: valid, 1: valid, 
def v(num):
    global descrip, rows, report, req
    req = set(['id','title','title_source','start'])
    fout=codecs.open(wd+files[num]+'_errors.json', 'w', encoding='utf8')
    report=validate_table(wd+files[num], 
                          schema=sch_new, 
                          #skip_errors=['missing-cell','non-matching-header'],
                          #skip_errors=['missing-cell'],
                          skip_errors=['missing-cell','missing-header','blank-header'],
                          sync_schema=True)
    if len(req - set(report.tables[0]['header'])) >0:
        return 'required column(s) is missing: '+ ", ".join(list(req - set(report.tables[0]['header'])))
    else:   
        descrip = describe(wd+files[num])
        rows = extract(wd+files[num])
    
        fout.write(json.dumps(report, indent=2))
        fout.close()
    
        return pp(report)
        #return pp({"header": report['tables'][0]['header'], 
                   #"errors": report['tables'][0]['errors']},indent=2)
        #fout.write(json.dumps(report['tables'][0]['errors'], indent=2))

# no skip_errors
greport=gvalidate(wd+files[3], schema=sch,
                     order_fields=True,
                     row_limit=20000,
                     skip_errors=['missing-header','missing-cell','non-matching-header'])
greport=gvalidate(wd+files[3], schema=sch_new, order_fields=True)
greport=gvalidate(wd+files[3], schema=sch_new, order_fields=True,skip_errors='missing-value')
errors = [x for x in greport['tables'][0]['errors'] if x['code'] not in ["blank-header","missing-header"]]
error_types = list(set([x['code'] for x in errors]))

fn=''
#greport=gvalidate(wd+files[2], schema=sch, 
                      #skip_errors=['missing-cell','non-matching-header'])
                      ##skip_errors=['missing-cell','missing-header'])
                      ##sync_schema=True)


#source, scheme=None, format=None, hashing=None, encoding=None, compression=None, compression_path=None, control=None, dialect=None, query=None, headers=None, schema=None, sync_schema=False, patch_schema=False, infer_type=None, infer_names=None, infer_volume=100, infer_confidence=0.9, infer_missing_values=[u''], onerror='ignore', lookup=None, checksum=None, extra_checks=None, pick_errors=None, skip_errors=None, limit_errors=None, limit_memory=1000

# from DatasetCreateView()
# tempfn e.g. '/var/folders/f4/x09rdl7n3lg7r7gwt1n3wjsr0000gn/T/tmp1zws3os2'
# returns validation result
#def validate_tsv(tempfn):