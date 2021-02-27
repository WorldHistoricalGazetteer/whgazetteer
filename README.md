## World Historical Gazetteer

This is the repository for the World Historical Gazetteer (WHG) web platform and API. The project is based at the University of Pittsburgh's [World History Center (WHC)](https://www.worldhistory.pitt.edu/). It has been substantially funded by a US National Endownment for the Humanities (NEH) grant and in-kind support from WHC.

WHG is aggregating attestations of historical place references contributed by researchers. [Version 1](http://whgazetteer.org) was launched on 27 July 2020 and incremental updates are ongoing. There is substantial information about WHG in its [Site Guide](http://whgazetteer.org/tutorials/guide/) and [Tutorials](http://whgazetteer.org/tutorials/). Announcements and discussion about the project is also available on [our blog website](http://blog.whgazetteer.org) and via our Twitter feed, [@WHGazetteer](https://twitter.com/WHGazetteer).

### Change log

#### v1.2 (25 Feb 2021)

- New local 3.5m record Wikidata index for reconciliation (beta; ask for access)
- New and improved authorization functions (register, login, password recovery)
- Add dataset collaborators in 'member' or 'co-owner' roles
- 

#### v1.1a (11 Jan 2021)

- Support for uploading & validating .csv, .xlsx, and .ods formats added
- Compute country codes if absent & geometry present
- Fixes: wikidata links; reporting upload errors; temporal data parsing 

#### v1.1 (10 Dec 2020)

- Reconciliation tasks now queued, and dataset owner notified by email upon completion.
- Base map replaced w/Natural Earth tiles
- Numerous minor bug fixes

#### v1.0 (27 July 2020)
- _Additional data accessioned:_ 28,000 historical records from Pleiades, Euratlas, and OWTRAD datasets.
- _APIs for access to the database and the Elasticsearch index._ Site search is against accessioned and conflated records in the index, but data uploaded to the database is discoverable as well, if flagged 'public'
- _Additional trace data added._ 70 new sets of annotations
- _Search function significantly updated._ Filters for feature class/category, spatial, temporal constraints.
- _Comprehensive Site Guide added._
- _Download datasets, before and after augmenting via reconciliation._
- _Recover password functionality._
- _Numerous GUI improvements and fixes._ Including redesign of home page.


#### beta v0.4 (2 May 2020)
- _Dataset downloads._ Options from dataset detail panel to download a) latest uploaded data file as is, b) augmented dataset (includes any geometries and/or concordance 'links' added during reconciliation review step).

- _Draft public API._ Parameters include q=, dataset=, ccodes=. E.g. `/api/?q=abydos&dataset=black&ccodes=eg`

- _Misc minor UI fixes_.

#### beta v0.3 (25 Feb 2020)
- _Core data significantly expanded._ Added ~1.5 million place records from Getty Thesaurus of Geographic Names (TGN), and ~1,000 large cities from GeoNames to WHG index. Contributions from historical sources will, in addition to adding novel entries, add "temporal depth" to these ahistorical data.

- _Dataset updates._ Dataset owners are now able to perform updates by uploading new data files. Analysis of the changes to be made (add, remove, replace) is performed automatically, and presented for review before performing them. _NOTE: available only for LP-TSV format currently. Linked Places format will follow soon, as will enhancements to file management._


- _"Collaborator" role._ Dataset owners can specify any number of registered users as collaborators, able to view non-public data and assist in review of reconciliation results; effectively, dataset project teams.

- _Undo last reconciliation match._ When moving quickly through prospective matches in the reconciliation review task, it is possible to inadvertently save a match; we've added a single step undo of the last action.

- _Dataset management functions consolidated._ Single page with tabs for Metadata, Browse, Reconciliation, Collaboration functionality.

### Software libraries and packages

- Django 2.2.10
- Python 3.7.4
- PostgreSQL 10
- Elasticsearch 6.6
- Nginx 1.14.0
- Gunicorn 19.9.0
- Celery 4.2.1
- Bootstrap 4
- JQuery 3.3.1
- Leaflet 1.3.1

