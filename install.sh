#!/bin/bash

# run this using sudo
apt install -y git
apt install -y python-is-python3
apt update
apt upgrade
apt install -y gdal-bin libgdal-dev gfortran gunicorn
apt install -y python3-pip
pip install -r requirements397.txt
