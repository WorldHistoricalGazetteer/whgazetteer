import sys, codecs, os, json

wd = '/Users/karlg/Documents/Repos/_whgazetteer/es/'
#file = wd+'trace_data/traces_20200702.json'
file = wd+'trace_data/traces_20200722_all.json'
fout = codecs.open(wd+'trace_places.tsv', mode='w', encoding = 'utf8')
# read from file 
infile = codecs.open(file, 'r', 'utf8')
trdata = json.loads(infile.read())

#places = []
for t in trdata[1:]:
  for b in t['body']:
    fout.write( str(b['place_id']) +'\t'+ b['title']+'\n')
fout.close()

#[[b['place_id'], b['title']] for b in t['body']]
