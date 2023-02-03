### How to create a local running instance of WHG v2.1

WHG v2.1 is a Django 2.2.28 application, and makes use of a number of addon packages. A running development instance requires a local PostgreSQL database (>= v10), and connects to production Elasticsearch indexes for search and reconciliation. 

The following steps have been tested on MacOS Monterey (12.6). Windows-specific modifications will be added soon.

- clone the [WHG repo](https://github.com/WorldHistoricalGazetteer/whgazetteer) from github into desired location
    - `git clone git@github.com:WorldHistoricalGazetteer/whgazetteer.git`
- `cd whgazetteer`

- create a Python 3.97 virtual environment & activate it, e.g.
  - `pyenv install 3.97` 
  - `virtualenv {envs path}/whg397`
- from project root, `source {envs path}/whg397/bin/activate`
- confirm 3.97 is active on command line: `python -V`
- configure your IDE to use that 3.97 Python environment
- create a debug file, e.g. from project root
  - `mkdir whg/logs`
  - `touch whg/logs/debug.log`
- install minimum Python packages from project root (e.g. myrepos/whgazetteer)
    - `python -m pip install -r reqmin.txt`
    - NB. The preliminary list of minimum requirements in __reqmin.txt__ may be adjusted after further testing
- create a new empty PostgreSQL database (>= v10) with postgis extension, e.g. using psql and a database name of "whg":
  - `psql -h localhost -p 5432 -d postgres`
  - postgres$ `CREATE DATABASE whg;`
  - `\c whg`
  - whg$ `CREATE EXTENSION postgis;`
- add settings to {project root}/whg/settings.py (contact karl [at] kgeographer.org)
    - SECRET_KEY
    - MAPBOX_TOKEN
    - ES_APIKEY_ID, ES_APIKEY_KEY (for Elasticsearch)
    - ES_CONN connection string
    - DATABASES (your local 'whg' as default)
    - DEBUG = True
    - URL_FRONT = 'http://localhost:8000/'
    - EMAIL settings
    - (possibly required) GDAL_LIBRARY_PATH, GEOS_LIBRARY_PATH
- build database tables from Django models; from project root:
  - `./manage.py makemigrations --dry-run`
  - this will preview the planned table-creation actions for the project, derived from the `models.py` files found in the project. 
  - `./manage.py makemigrations` (generates migration files per model)
  - `./manage.py migrate` (builds tables from migration files just created)`
- create superuser account
  - {project root}$ `python manage.py createsuperuser`
- start dev server and confirm model in the site's admin app
  - {project root}$ `./manage.py runserver {port}`
  - in a browser, navigate to `http://localhost:{port}/admin`
  - log in as {superuser} 
  - confirm model pages (datasets, places, etc.). There is no data in them yet.
- populate a few database tables for basic operation (home page looks for these), e.g.
    - `psql -p 5432 {database} < docs/cloning/types.sql`
    - `psql -p 5432 {database} < docs/cloning/combined.sql`
- from project root, gather static files from throughout project into static/ 
  - {project root}$ `python ./manage.py collectstatic`
- home page should now load at `http://localhost:{port}`
- navigate to My Datasets, My Collections, confirm 2 of each
- add new sample dataset, `docs/cloning/sample7.txt`
- the site **should** be largely functional; please report issues to KG

#### NOTES re: reconciliation and data downloads
- The Celery python package is used to manage task queues, and Celery relies on the redis library.
    - Celery is run as a separate process, and needs to be started alongside the Django server
        - from the project root, run: celery -A whg worker -l info
    - redis is a message broker library, its python package was installed via pip
    - Celery will look for a redis server on its default port, 6379
        - Celery finds all views in the project tagged with the @shared_task decorator
    - redis *may* need to be installed as a service on your system.
        https://redis.io/topics/quickstart
        windows: https://redis.io/docs/getting-started/installation/install-redis-on-windows/
    -

