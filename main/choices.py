# datasets model field value choices

COMMENT_TAGS = [
    ('bad_link','Incorrect match/link'),
    ('bad_type','Incorrect place type'),
    ('typo','Typo'),
    ('other','Other'),
]

FORMATS = [
    ('lpf', 'Linked Places v1.0'),
    ('delimited', 'LP-TSV')
]

DATATYPES = [
    ('place', 'Places'),
    ('anno', 'Traces'),
    #('source', 'Sources')
]

STATUS = [
    ('format_error', 'Invalid format'),
    ('format_ok', 'Valid format'),
    ('in_database', 'Inserted to database'),
    ('uploaded', 'File uploaded'),
    ('ready', 'Ready for submittal'),
    ('accessioned', 'Accessioned'),
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
    'align_whg':'whg:'
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