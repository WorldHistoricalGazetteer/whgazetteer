# DBpedia alignment 27 Oct 2017; modified 5 May 2019

from SPARQLWrapper import SPARQLWrapper, JSON
import sys, os, re, json, codecs, time, datetime
import shapely.geometry
from geopy import distance
from align_utils import classy, roundy, fixName

endpoint_dbp = "http://dbpedia.org/sparql"
sparql = SPARQLWrapper(endpoint_dbp)

start = time.time()
timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M"); print(timestamp)

dataset = 'dplace' # ['black','dplace','voy']

# IN
fin = codecs.open('../../_whgdata/pyin/'+dataset+'_for-align.json', 'r', 'utf8')
rows = fin.readlines()
fin.close()

fincc = codecs.open('../../_whgdata/pyin/parents.json', 'r', 'utf8') # iso: {"name":__,"geonameid":___}
parents = json.loads(fincc.read())
fincc.close()

# OUT: hits
fout1 = codecs.open('../../_whgdata/pyout/align_dbp/'+dataset+'/es-hits_'+timestamp+'.txt', 'w', 'utf8')
fout1.write('placeid\tdist\tprefname\twhg_types\tminmax\tdbp_label\tdbp_item\tccode_whg\tlat\tlon\tgeom\tsnippet\tsnippet_lang\n')

# misses, skipped
fout2 = codecs.open('../../_whgdata/pyout/align_dbp/'+dataset+'/es-missed_'+timestamp+'.txt', 'w', 'utf8')
fout3 = codecs.open('../../_whgdata/pyout/align_dbp/'+dataset+'/es-skipped_'+timestamp+'.txt', 'w', 'utf8')

count_hits = 0
count_misses = 0
count_redir = 0

for x in range(len(rows)):
    # to json
    row = json.loads(rows[x])
    
    # extract search parameters
    qname = fixName(row['prefname']) 
    
    # country-codes.json
    #try:
    cntry = parents['ccodes'][row['countries'][0]]['gnlabel'] if len(row['countries']) > 0 else ''
    #except:
        #fout3.write(str(row['blackid']) + '\t' + row['toponym'] + '\t' + str(sys.exc_info()[0]) + '\n')
        #continue        

    # TODO (tgn): find and use temporal info where exists
    minmax = [] if row['minmax'] == None else row['minmax']
    
    placetype = classy('dbp',row['placetypes'])[0]
    #print(search_name, placetype, cntry, minmax)

    location = tuple([row['centroid'][1],row['centroid'][0]])
    
    #variable insert for type
    if dataset == 'black':
        alt_types = ''' || ?itemType = geo:SpatialThing || ?itemType = yago:YagoGeoEntity ||
                    ?itemType = yago:Group100031264 || ?itemType = dbo:EthnicGroup || ?itemType = yago:State100024720
                    '''
    elif dataset == 'dplace':
        alt_types = ''' || ?itemType = yago:Group100031264 || ?itemType = dbo:EthnicGroup'''
    else:
        alt_types = ''
                  
    def runQuery(qtype,qname):
        global count_hits, count_redir, count_misses
        qi = '''
            SELECT DISTINCT ?item ?itemLabel ?redirectsTo COALESCE(?lat,'') AS ?lat 
            COALESCE(?lon,'') AS ?lon ?abstract COALESCE(?geom,'') AS ?geom
             WHERE { 
                ?item rdfs:label ?itemLabel .
                FILTER ( ?itemLabel = "%s"@en )
                ?item rdf:type ?itemType .
                FILTER ( ?itemType = dbo:%s %s)
                ?item dbo:abstract ?abstract .
                FILTER (lang(?abstract) = 'en')
                OPTIONAL {
                   ?item dbo:country ?country .
                   ?country rdfs:label ?cntryLabel .
                   FILTER (?cntryLabel  = "%s"@en) 
                   }
                OPTIONAL {?item geo:geometry ?geom . }
                OPTIONAL {?item geo:lat ?lat . }
                OPTIONAL {?item geo:long ?lon . }
                OPTIONAL {?item dbo:wikiPageRedirects ?redirectsTo .}
            }
            ORDER BY ?itemLabel
        ''' % (qname, placetype, alt_types, cntry)   
        
        qr = '''
            SELECT ?redirectsTo WHERE {
                ?x rdfs:label "%s"@en .
                ?x dbo:wikiPageRedirects ?redirectsTo
            }
        ''' % (qname)
        q = qi if qtype == 'qi' else qr

        # run initial query
        sparql.setQuery(q)
        sparql.setReturnFormat(JSON)

        bindings = sparql.query().convert()["results"]["bindings"]
        if len(bindings) > 0:
            count_hits +=1 # there's a hit
            # whgid;toponym;whg_types;minmax;dbp_label;dbp_item;ccode_whg;lat;long;eom;snippet;snippet_lang
            # get distance
            if bindings[0]['lat']['value'] != '':
                latlon = (bindings[0]['lat']['value'],bindings[0]['lon']['value'])
                dist = int(distance.distance(latlon,location).km)
            else:
                dist = '?km'
            print(dist)
            hit = str(row['placeid'])+'\t'+ \
                dist+'\t'+ \
                row['prefname']+'\t'+ \
                str(row['placetypes'])+'\t'+ \
                str(minmax)+'\t'+ \
                bindings[0]['itemLabel']['value']+'\t'+ \
                bindings[0]['item']['value']+'\t'+ \
                str(row['countries'])+'\t'+ \
                bindings[0]['lat']['value']+'\t'+ \
                bindings[0]['lon']['value']+'\t'+ \
                bindings[0]['geom']['value']+'\t'+ \
                bindings[0]['abstract']['value']+'\t'+ \
                bindings[0]['abstract']['xml:lang']+'\n'            
            fout1.write(hit)
        else:
            # no hit - is there a redirect?
            #print('no hit, qr= ',qr)
            sparql.setQuery(qr)
            sparql.setReturnFormat(JSON)
            bindings = sparql.query().convert()["results"]["bindings"]
            #print('redir bindings',bindings, len(bindings))
            if len(bindings) > 0:
                # yes there is a redirect
                count_redir +=1
                redir = bindings[0]['redirectsTo']['value']
                label = re.search('resource\/(.*?)$',redir).group(1).replace('_',' ')
                #print('redir to: ',label)
                runQuery('qi',label)
                #fout3.write(json.dumps(bindings[0]) + '\n')
            else:
                # give up for now
                fout2.write(json.dumps(row,ensure_ascii=False)+'\n')
                count_misses += 1
                #print('no hits for: ',search_name, placetype, cntry, minmax)
                #print(bindings)            
    try:
        runQuery('qi',qname)
    except:
        fout3.write(str(row['placeid']) + '\t' + row['prefname'] + '\t' + str(sys.exc_info()[0]) + '\n')
        continue
print(count_hits,' hits; ',count_redir, 'redirects; ', count_misses, 'misses')
fout1.close()
fout2.close()
fout3.close()

end = time.time()
print('elapsed time in minutes:',int((end - start)/60))
