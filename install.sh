#!/bin/bash

# run this using sudo
apt install -y git wget
apt install -y python-is-python3
apt update
apt upgrade
apt install -y gdal-bin libgdal-dev gfortran gunicorn
apt install -y python3-pip
pip install -r requirements397.txt
wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O cloud_sql_proxy
chmod +x cloud_sql_proxy
apt install -y htop
export PGPASSWORD=<password>
psql -h localhost -d mehdie -U mehdie -p 5432 -a -w -f postgres_init.sql
