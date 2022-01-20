#!/bin/bash

cd django_facebook
touch /var/log/django.log
ln -sf /proc/1/fd/1 /var/log/django.log
#conda create -n streaming python=3 -y
#source activate streaming
#pip3 install -r ../requirements.txt
python3 manage.py runserver 0.0.0.0:8000 >> /var/log/django.log
