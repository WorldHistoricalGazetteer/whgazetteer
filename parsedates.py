from datetime import date,datetime
import dateutil.parser
import re
ex=[
    ['1831-06','1831-09'],
    ['1831-06','1832-06/1832-09'],
    ['1831','1831'],
    ['1831','1838'],
    ['1831/1832','1840'],
    ['1831/1832','']
]
ry=re.compile('(-?\d{1,4})')
rd=re.compile('(-?\d{1,4})(-(\d{1,2}))?(-(\d{1,2}))?')

date(2002, 12).isoformat()
date = datetime.fromisoformat(ex[0][0]);print(date)
newdate = dateutil.parser.parse(ex[0][0])

ts=ex[0]
for e in ex:  
    for d in e:
        year = re.match(ry,d).group(1)
        dg = list(re.match(rd,d).groups())
        print(year,dg)
    
def parsedates_tsv(arr):
    def intmap(arr):
        return [int(a) for a in arr]
    union = [*set(arr[0].split('/')), *set(arr[1].split('/'))]
    union = [term for term in union if term not in [None,'','..']]
    union = intmap(union)
    
    return {'minmax':[min(union),max(union)]}
    #return {"timespan":{"start": {"":""}, "end": {"":""}},
          #"minmax":[min(union),max(union)]}

for e in ex:
    print(parsedates_tsv(e))