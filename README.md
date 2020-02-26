## World Historical Gazetteer

This is the repository for the World Historical Gazetteer (WHG) web platform and API. The project is based at the University of Pittsburgh's [World History Center](https://www.worldhistory.pitt.edu/), and has been substantially funded by the US National Endownment for the Humanities (NEH).

WHG is aggregating attestations of historical place references contributed by researchers. The [Beta release v0.3](http://dev.whgazetteer.org) was made available on 25 February 2020. More information on the project is available on [our blog website](http://whgazetteer.org).

### Change log

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

