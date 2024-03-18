## The World Historical Gazetteer Platform

### v2.1 Stack (_v3 versions_)

----------
- Django 2.2 (4.1)
- Python 3.9.7 (3.10.7)
- PostgreSQL 15
- Elasticsearch 7.17.1 (8.6.2)
- Kibana 7.17.1 (8.6.2)
- Celery 4.4.7 (5.2.7)
- Redis 3.5.3 (4.5.2)
- Nginx on Ubuntu 22.10 @ Digital Ocean
- _see req397.txt_

### Frontend

--------
- Bootstrap 4.6.0 (5.2.3)
- JQuery 3.6.0 (3.6.3)
- JQuery UI 1.13.2
- Plain old vanilla JavaScript
- Leaflet (django-leaflet; numerous add-ons)
- Mapbox GL JS 1.15.0 
- MapLibre GL JS 1.15.2
- Turf JS

### Functionality

----------
- Anonymous users can browse the site, view public data

- The main data store is a PostgreSQL database 'whgv2'
    - As of July 2023 there are 2.325 million place records in the database, 1.95m of those are public, and have among them 3.17m names. Of those, 1.92m are linked within the WHG "union index."
- Django "apps" folders organize the code generally: whg (root), datasets, places, collection, resources, search, areas, api, elastic, accounts, allauth
- Registered users can upload data files to db as new "datasets", and work with them in their private workspace
- Uploaded data must be in either [Linked Places](https://github.com/LinkedPasts/linked-places-format) or [LP-TSV](https://github.com/LinkedPasts/linked-places-format/blob/main/tsv_0.4.md) format
- Upon upload, data is validated; if invalid, errors may or may not be returned to GUI. If not, email goes to user and site admin: 'look into this'

  - validation uses json schemas (+ frictionless.py for delimited) and is buggy
  - data from either format is written to a normalized database schema: *places*, and *place\_name*, *place\_type*, *place\_geom*, etc. with FK to *places*.
- **Contribution pipeline** (users or WHG staff):
  - upload file to create dataset
  - reconcile data against internal Wikidata index (3.5m place records; 2021)
          - a Celery task, email to user when complete 
  - review reconciliation results, decide on candidate matches
  - request publication (editor ensures adequate metadata)
  - datasets augmented with geometry and authority identifiers from the reconciliation process can be downloaded at any time for various uses.
  - "accessioning": reconcile against WHG "union index," linking individual records across datasets using Elasticsearch parent/child pattern
        - a Celery task, email to user when complete 
  - review accessioning candidate matches until complete
  - dataset owners can add registered users in either a "member" or "co-owner" role, e.g. to assist with the review step.
- **Collections**:
  - registered users can create 2 kinds of collections - private at first, then ask that they be made public 
    - _Place Collections_ are sets of place records in the system drawn from any datasets. They are especially applicable to teaching scenarios and features for managing classes and/or workshops are in development. Records in a Place Collection can be annotated with comments, dates, keywords, and the collection itself typically includes an image, multiple external links, and a PDF essay. 
    - _Dataset Collections_ are sets of datasets, and a means for an individual or a group creating a "Gazetteer of {_some region_}"
  - significant enhancements to both kinds of collections are in development for v3 (alpha early 2024)
- **Search**: there are two modes of search...
  - _against the union index_ (searches "parent" records, returns parents and children)
        - results are listed and each links to a Place Portal page, where all indexed attestations for a place are assembled
  - _against the database_ (public records only; much slower)
      - each database record has its own landing page (e.g. [Abydos](https://whgazetteer.org/places/81010/detail))
- **Browse**:
  - Public datasets are listed and each has a browse screen w/tabs for a) metadata and b) records table + map
    - Each individual place in a public dataset has its own display page
    - Dataset records can be downloaded in JSON and in cases TSV format
  - Public collections are listed and each has a public display screen listing places and with an interactive map
- **API**
  - There are several API endpoints, and requests for others are welcome
  - Some endpoints are for use by a plugin available for the RecogitoJS and AnnotoriousJS annotation libraries (to date tested but not used)
- **Teaching**
  - There is a page listing several lesson plans contributed by teachers

### How it works

------------
- As a Django project, code is organized into "app" folders, and all interaction with the database is initiated with calls in .html pages to the URLs found in each app's _urls.py_ file. These execute "views," typically found in the app's _views.py_ files. Django views are functions; some are "class-based." All perform some actions and (typically) redirect to .html "template" files, delivering content and miscellaneous "context" items for use in the templates. Most class-based views are specialized Django classes that perform standard CRUD functions. Function-based views are bespoke.
- The _DjangoRestFramework_ package is used mostly for the API and some serializers. The _rest-framework-datatables_ package is used in several places. All 3rd-party modules in use are listed in _whg/settings.py_.
- There are many helper functions, found in _views.py_ files and some supporting _\*\_utils.py_ files.
- The _datasets/static/\*_ folders have some dict lookup files and validation schemas
- The _datasets/templatetags_ folder has a number of helper functions used in .html templates
- Functions managed by Celery -- e.g. executing reconciliation against Elasticsearch indexes -- are found in _datasets/tasks.py_, and are prefixed by a _@shared\_task_ decorator. 
- Each app's _models.py_ file has class definitions for entities managed in the database: e.g. Place (and PlaceName, PlaceType, etc.), Dataset, Area, Resource, PlaceCollection, DatasetCollection and so forth. Many model classes include "@property" functions which are essentially custom methods on the class.
- All .html files "extend" a _main/templates/main/base.html_ file, which has the main menu and loads libraries used by most or all pages.
- Most .html templates also contain many Javascript functions that handle events on DOM elements; some are ajax calls to Django URLs and return data; some simply perform some action in the page.
- No Javascript frameworks are involved in WHG (React, Angular, Vue, etc.)
- As datasets move through the pipeline, changes are made to their _status_ field, e.g. _uploaded_, _reconciling_, _wd-complete_, _accessioning_, _indexed_. Some changes in status triggere emails to staff and/or contributors.
- Almost all .html layout and graphical controls are Bootstrap: grid, tabs, buttons, etc.
- There are no functioning tests to speak of :^(

### Contributing code

-------------
It is possible to stand up a functioning instance of the WHG codebase, and proposals for tasks to undertake will be considered promptly! It will soon be 'dockerized' but in the meantime it can be run locally, sufficient for development. The process has only been tested once, so there are likely to be a few wrinkles.

- Instructions for this are found in _docs/cloning/cloning.md_
- It will be necessary to 
  - run PostgreSQL locally, and use a Python virtual environment (3.9.7) loaded up with everything in req397.txt
  - build the database tables with _makemigrations_ and _migrate_ commands

  - add some dummy data from SQL files so the home page displays

  - to work on features like mapping, some sizable datasets should be added, by uploading data files downloaded from the production site (or provided on request).
- The instance can read from the production Elasticsearch instance for search and reconciliation, but permissions will necessarily prevent any writes.

### Version 3

------------
Version 3 is in active development, with many significant changes and additions, so it may make sense to work on that codebase instead of Version 2.1. Please get in touch to discuss options.