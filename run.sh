#!/bin/bash

./cloud_sql_proxy -instances="islam-db:europe-west4:mehdiepostgis10-main"=tcp:5432 &
authbind gunicorn --bind 0.0.0.0:8000 --workers 1 --threads 8 --timeout 0 whg.wsgi:application &
