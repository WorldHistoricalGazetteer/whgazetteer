# Wikidata alignment 21 June 2018 rev. 5 May 2019
# 
from SPARQLWrapper import SPARQLWrapper, JSON
import sys, os, re, json, codecs, time, datetime
from time import sleep
import shapely.geometry
from geopy import distance
from align_utils import classy, roundy, fixName

endpoint_dbp = "http://dbpedia.org/sparql"
endpoint_wd = "https://query.wikidata.org/sparql"
sparql = SPARQLWrapper(endpoint_wd)

start = time.time()
timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M"); print(timestamp)

dataset = 'indias' # ['black','dplace','voy','indias']
os.chdir('/Users/karlg/Documents/Repos/_whgdata/')
# IN
fin = codecs.open('pyin/'+dataset+'_for-align.json', 'r', 'utf8')
rows = fin.readlines()
fin.close()

fincc = codecs.open('pyin/parents.json', 'r', 'utf8') # iso: {"name":__,"geonameid":___}
parents = json.loads(fincc.read())
fincc.close()

# OUT: hits
#fout1 = codecs.open('pyout/align_wd/'+dataset+'/es-hits_'+timestamp+'.txt', 'w', 'utf8')
#fout1.write('placeid\tqname\twd_label\twd_uri\tgnid\ttgnid\tdistance\n')

# misses, skipped
#fout2 = codecs.open('pyout/align_wd/'+dataset+'/es-missed_'+timestamp+'.txt', 'w', 'utf8')
#fout3 = codecs.open('pyout/align_wd/'+dataset+'/es-skipped_'+timestamp+'.txt', 'w', 'utf8')
#fout4 = codecs.open('pyout/align_wd/'+dataset+'/es-multi_'+timestamp+'.txt', 'w', 'utf8')

count_hits = 0
count_multi = 0
count_misses = 0
count_skipped = 0

def toWKT(coords):
    wkt = 'POINT('+str(coords[0])+' '+str(coords[1])+')'
    return wkt
    
#for x in range(len(rows)):
for x in range(0,10):
    # to json
    row = json.loads(rows[x])
    
    # extract search parameters
    qname = fixName(row['prefname']) 
    
    #altnames = row['altnames']
    altnames = ', '.join(['"'+i+'"' for i in row['altnames']])

    # not all data have coordinates
    location = toWKT(row['geom']['coordinates'][0])
    
    # country-codes.json
    #cntry = parents['ccodes'][row['countries'][0]]['gnlabel'] if len(row['countries']) > 0 else ''
    #placetype = classy('dbp',row['placetypes'])[0]

    # TODO: find and use temporal info where exists
    minmax = [] if row['minmax'] == None else row['minmax']
    
    def runQuery():
        global count_hits, count_redir, count_misses, count_multi
        #print(qname)
        q='''SELECT distinct ?place ?location ?distance ?placeLabel ?tgnid ?viafid ?bnfid ?gnid WHERE {
            SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
            SERVICE wikibase:around { 
              ?place wdt:P625 ?location . 
              bd:serviceParam wikibase:center "%s"^^geo:wktLiteral .
              bd:serviceParam wikibase:radius "50" . 
              bd:serviceParam wikibase:distance ?distance .
            } 
        
            ?place rdfs:label ?placeLabel ;
                (wdt:P31/wdt:P279*) ?placeType .
                
            FILTER (?placeType = wd:Q486972 || ?placeType = wd:Q839954) .
            FILTER (STR(?placeLabel) in (%s)) .
        
            # external IDs
            OPTIONAL {?place wdt:P1667 ?tgnid .}
            OPTIONAL {?place wdt:P1566 ?gnid .}
            OPTIONAL {?place wdt:P214 ?viafid .}
            OPTIONAL {?place wdt:P268 ?bnfid .}
            OPTIONAL {?place wdt:P244 ?locid .}
        } ORDER BY ?placeLabel'''% (location, altnames) 
            # {?place wdt:P244 ?locid .}
        
        # set query
        sparql.setQuery(q)
        sparql.setReturnFormat(JSON)

        # run it
        bindings = sparql.query().convert()["results"]["bindings"]

        # test, output results
        if len(bindings) > 1:
            count_multi +=1 # multiple hits
            for b in bindings:
                #print(b)
                gnid = b['gnid']['value'] +'\t' if 'gnid' in b.keys() else '\t'
                tgnid = b['tgnid']['value'] +'\t' if 'tgnid' in b.keys() else '\t'
                hit = str(row['placeid'])+'\t'+ \
                    row['prefname']+'\t'+ \
                    b['placeLabel']['value']+'\t'+ \
                    b['place']['value']+'\t'+ \
                    gnid+tgnid + \
                    b['distance']['value']                
                fout4.write(hit + '\n')
            fout4.write('\n')
            
        elif len(bindings) == 1:
            count_hits +=1 # there's a single hit
            # fout1.write('placeid\tqname\twd_label\twd_uri\tgnid\ttgnid\tdistance\n')
            gnid = bindings[0]['gnid']['value'] +'\t' if 'gnid' in bindings[0].keys() else '\t'
            tgnid = bindings[0]['tgnid']['value'] +'\t' if 'tgnid' in bindings[0].keys() else '\t'
            hit = str(row['placeid'])+'\t'+ \
                qname+'\t'+ \
                bindings[0]['placeLabel']['value']+'\t'+ \
                bindings[0]['place']['value']+'\t'+ \
                gnid+tgnid + \
                bindings[0]['distance']['value']
            fout1.write(hit + '\n')
            
        elif len(bindings) == 0:
            count_misses +=1   
            fout2.write(str(row['placeid'])+'\n')

    try:
        runQuery()
    except:
        count_skipped +=1
        fout3.write(str(row['placeid']) + '\t' + row['prefname'] + '\t' + str(sys.exc_info()[0]) + '\n')
        continue
    
print(count_hits,' hits; ',count_multi, 'multi; ', count_misses, 'misses', count_skipped, 'skipped')
#fout1.close()
#fout2.close()
#fout3.close()
#fout4.close()

end = time.time()
print('elapsed time in minutes:',int((end - start)/60))



# TODO: allow more than human settlements & archaeological sites
#variable insert for type
#if dataset == 'black':
    #alt_types = ''' || ?itemType = geo:SpatialThing || ?itemType = yago:YagoGeoEntity ||
                #?itemType = yago:Group100031264 || ?itemType = dbo:EthnicGroup || ?itemType = yago:State100024720
                #'''
#elif dataset == 'dplace':
    #alt_types = ''' || ?itemType = yago:Group100031264 || ?itemType = dbo:EthnicGroup'''
#elif dataset == 'indias':
    #alt_types = ''' || ?itemType = geo:SpatialThing || ?itemType = yago:YagoGeoEntity ||
                #?itemType = yago:Group100031264 
                #'''
#else:
    #alt_types = ''
              
