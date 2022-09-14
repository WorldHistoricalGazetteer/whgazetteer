#!/bin/bash

./cloud_sql_proxy -instances="islam-db:europe-west4:mehdiepostgis10-main"=tcp:5432 &
# run this using sudo
