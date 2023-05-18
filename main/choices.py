# model field value choices

AREATYPES = {
    ('ccodes','Country codes'),
    ('copied','CopyPasted GeoJSON'),
    ('search','Region/Polity record'),
    ('drawn','User drawn'),
    ('predefined','World Regions'),
}

AUTHORITIES = [
    ('tgn','Getty TGN'),
    ('dbp','DBpedia'),
    ('gn','Geonames'),
    ('wd','Wikidata'),
    ('core','WHG Spine'),
    ('whg','WHG'),
]

AUTHORITY_BASEURI = {
    'align_tgn':'tgn:',
    'align_dbp':'dbp:',
    'align_gn':'gn:',
    'align_wd':'wd:',
    'align_wdlocal':'wd:',
    'align_idx':'whg:',
    'align_whg':'whg:'
}

COLLECTIONCLASSES = [
    ('dataset','Dataset collection'),
    ('place','Place collection'),
]

COLLECTIONGROUP_TYPES = [
    ('class', 'Class'),
    ('workshop', 'Workshop')
]

COLLECTIONTYPES = [
    ('event','Event'),
    ('person','Person'),
    ('work','Work'),
    ('theme','Theme'),
]

COMMENT_TAGS = [
    ('typo','Typo'),
    ('geom_error','Geometry error'),
    ('misplaced','Record does not belong in set'),
    ('other','Other'),
]
COMMENT_TAGS_REVIEW = [
    ('typo','Typo'),
    ('geom_error','Geometry error'),
    ('defer', 'Deferring (in review)'),
    ('other','Other'),
]

DATATYPES = [
    ('place', 'Places'),
    ('anno', 'Traces')
]

ERAS = [
    ('ce', 'CE'),
    ('bce', 'BCE'),
]

# geonames classes for api filter
FEATURE_CLASSES = [
    ('A','Administrative divisions'),
    ('H','Hydrological features'),
    ('L','Landscape, regions'),
    ('P','Populated places (settlements)'),
    ('R','Roads, routes, transportation'),
    ('S','Sites (various)'),
    ('T','Topographical features'),
    ('U','Undersea features'),
    ('V','Vegetation landcover'),
]

#A: country, state, region,...
#H: stream, lake, ...
#L: parks,area, ...
#P: city, village,...
#R: road, railroad 
#S: spot, building, farm
#T: mountain,hill,rock,... 
#U: undersea
#V: forest,heath,...

FORMATS = [
    ('delimited', 'Delimited/Spreadsheet'),
    ('lpf', 'Linked Places v1.2'),
    ('dummy', 'empty file')
]

LINKTYPES = [
    # ('page','web page'),
    ('webpage',"<i class='fas fa-window-maximize'></i>"),
    ('image','<img src="/static/images/noun-photo.svg" width="16"/>') ,
    ('document',"<i class='far fa-file-pdf linky'></i>")
]

LOG_CATEGORIES = [
    ('user','User'),
    ('dataset','Dataset'),
    ('collection','Collection')
]

LOG_TYPES = [
    ('ds_create','Create dataset'),
    ('ds_update','Update dataset'),
    ('ds_delete','Delete dataset'),
    ('ds_recon','Reconciliation task'),
    ('area_create','Create area'),
    ('area_update','Update area'),
    ('area_delete','Delete area'),
    ('collection_create','Create collection'),
    ('collection_update','Update collection'),
    ('collection_delete','Delete collection'),
    ('annotation','Create/update annotation'),
]

MATCHTYPES = [
    ('close','closeMatch'),
    ('exact','exactMatch'),
    ('related','related'),
]

PASSES = [
    ('pass1', 'Query pass 1'),
    ('pass2', 'Query pass 2'),
    ('pass3', 'Query pass 3'),
]

REGIONS = (
    (72, 'Antarctica'),
    (73, 'Asiatic Russia'),
    (74, 'Australia/New Zealand'),
    (75, 'Caribbean'),
    (76, 'Central America'),
    (77, 'Central Asia'),
    (78, 'Eastern Africa'),
    (79, 'Eastern Asia'),
    (80, 'Eastern Europe'),
    (81, 'European Russia'),
    (82, 'Melanesia'),
    (83, 'Micronesia'),
    (84, 'Middle Africa'),
    (85, 'Northern Africa'),
    (86, 'Northern America'),
    (87, 'Northern Europe'),
    (88, 'Polynesia'),
    (89, 'South America'),
    (90, 'Southeastern Asia'),
    (91, 'Southern Africa'),
    (92, 'Southern Asia'),
    (93, 'Southern Europe'),
    (94, 'Western Africa'),
    (95, 'Western Asia'),
    (96, 'Western Europe'),
    (99, 'Global')
)

RESOURCE_TYPES = [
    ('Lesson plan', 'Lesson plan'),
    ('Syllabus', 'Syllabus'),
    ('Other', 'Other')
]
RESOURCE_FORMATS = [
    ('pdf', 'PDF'),
]
RESOURCE_STATUS = [
    ('uploaded', 'File uploaded'),
    ('published', 'Published')
]
RESOURCEFILE_ROLE = [
    ('primary', 'Primary file'),
    ('supporting', 'Supporting file')
]

STATUS_COLL = [
    ('sandbox', 'Sandbox'),
    ('demo', 'Demo'),
    ('group', 'Group'),
    ('ready', 'Ready'),
    ('published', 'Published'),
]

STATUS_DS = [
    ('format_error', 'Invalid format'),
    ('format_ok', 'Valid format'),
    ('remote', 'Created remotely'),
    ('uploaded', 'File uploaded'),
    ('reconciling', 'Reconciling'),
    ('wd-complete', 'Wikidata recon completed'),
    ('accessioning', 'Accessioning'),
    ('indexed', 'Indexed'),
]

STATUS_FILE = [
    ('format_ok', 'Valid format'),
    ('uploaded', 'File uploaded'),
    ('dummy', 'Dummy for remote'),
]

STATUS_HIT = [
    ('match', 'Match'),
    #('nomatch', 'No Match'),
]

STATUS_REVIEW = [
    #null = 'No hits'
    (0, 'Unreviewed'),
    (1, 'Reviewed'),
    (2, 'Deferred')
]

TEAMROLES = [
    ('creator', 'Creator'),
    ('owner', 'Owner'),
    ('member', 'Team Member'),
]

TRACERELATIONS = [
    ('subject','Subject'),
    ('waypoint','Waypoint'),
    ('locale','Locale'),
    ('birthplace','Birth place'),
    ('deathplace','Death place'),
    ('resided','Resided'),
    ('taught','Taught'),
    ('enlightened','Enlightened'),
    ('findspot','Findspot'),
    ('ruled','Ruled')
]

TRACETYPES = [
    ('person','Person'),
    ('dataset','Dataset'),
    ('event','Event'),
    ('journey','Journey'),
    ('work','Work'),
    ('concept','Concept')
]

USER_ROLE = (
    ('normal', 'normal'),
    ('group_leader', 'group leader'),
    ('superuser', 'superuser'),
)

USERTYPES = [
    ('individual', 'Individual'),
    ('group', 'Group or project team')
]

