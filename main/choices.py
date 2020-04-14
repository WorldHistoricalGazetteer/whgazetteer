# datasets model field value choices

LOG_CATEGORIES = [
    ('user','User'),
    ('dataset','Dataset')
]

LOG_TYPES = [
    ('ds_create','Create dataset'),
    ('ds_update','Update dataset'),
    ('ds_delete','Delete dataset'),
    ('ds_recon','Reconciliation task'),
    ('area_create','Create dataset'),
    ('area_update','Update dataset'),
    ('area_delete','Delete dataset'),
]

COMMENT_TAGS = [
    ('bad_link','Incorrect match/link'),
    ('bad_type','Incorrect place type'),
    ('typo','Typo'),
    ('other','Other'),
]

FORMATS = [
    ('lpf', 'Linked Places v1.1'),
    ('delimited', 'LP-TSV')
]

DATATYPES = [
    ('place', 'Places'),
    ('anno', 'Traces')
]

STATUS = [
    ('format_error', 'Invalid format'),
    ('format_ok', 'Valid format'),
    ('in_database', 'Inserted to database'),
    ('uploaded', 'File uploaded'),
    ('ready', 'Ready for submittal'),
    ('accessioning', 'Accessioning'),
    ('accessioned', 'Accessioned'),
    ('indexed', 'Indexed'),
]

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
    'align_whg':'whg:',
    'align_whg_b':'whg:'
}

MATCHTYPES = {
    ('exact','exactMatch'),
    ('close','closeMatch'),
    ('related','related'),
}

AREATYPES = {
    ('ccodes','Country codes'),
    ('copied','CopyPasted GeoJSON'),
    ('search','Region/Polity record'),
    ('drawn','User drawn'),
    ('predefined','World Regions'),
}

TRACETYPES = [
    ('person','Person'),
    ('dataset','Dataset'),
    ('event','Event'),
    ('journey','Journey'),
    ('work','Work')
]

TRACERELATIONS = [
    ('subject','Subject'),
    ('waypoint','Waypoint'),
    ('birthplace','Birth place'),
    ('deathplace','Death place'),
    ('resided','Resided'),
    ('taught','Taught'),
    ('enlightened','Enlightened'),
    ('findspot','Findspot'),
    ('ruled','Ruled')
]

USERTYPES = [
    ('individual', 'Individual'),
    ('group', 'Group or project team')
]

TEAMROLES = [
    ('creator', 'Creator'),
    ('owner', 'Owner'),
    ('member', 'Team Member'),
]

PASSES = [
    ('pass1', 'Query pass 1'),
    ('pass2', 'Query pass 2'),
    ('pass3', 'Query pass 3'),
]