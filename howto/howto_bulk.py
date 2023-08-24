# -*- coding: utf-8 -*-
"""
Sample script demonstrating xelastic bulk indexing

Assumed that an index template is created (see howto_templates.py):
    customer
    street_address
    email
    phone
    created

PUT _index_template/template-xelastic
{
    "index_patterns": "ta-cst-*",
    "template": {
        "settings": {
            "index": {"number_of_shards": "1", "number_of_replicas": "0"}
        },
        "mappings": {
            "dynamic": "strict",
            "properties": {
                  "name": {"type": "keyword"},
                  "email": {"type": "keyword"},
                  "phone": {"type": "keyword"},
                  "created": {"type": "date", "format": "epoch_second"}
            }
        },
        "aliases": {}
    },
    "priority": 500,
    "_meta": {
        "description": "xelastic test data"
    },
    "version": 1
}

Created on Thu Mar  9 14:14:41 2023

@author: juris.rats
"""
import time
import logging
import sys
import os
#sys.path.append("C:\\Users\\juris.rats\\AppData\\Local\\miniconda3\\Lib\\site-packages")

sys.path.append("..")
from src.xelastic import XElastic, XElasticIndex, XElasticBulk

logging.basicConfig(handlers=[logging.StreamHandler()], level=logging.INFO,
    format= "%(asctime)s [%(filename)s:%(lineno)s - %(funcName)s() ] %(message)s")

conf = {
    'connection': {'client': os.environ.get('ELASTICSEARCH_URL')},
    'prefix': 'ta',
    'source': 'src',
    'timeout': 10,
    'indexes': {
        'customers': {'stub': 'cst', 'span_type': 'm', 'date_field': 'created'}}
   }

items = [
    {"name": "John", "email": "john@xelastic.com", "phone": "12345678",
        "group": "A"},
    {"name": "Jane", "email": "john@xelastic.com", "phone": "234567811",
        "group": "A"},
    {"name": "Doris", "email": "doris@xelastic.com", "phone": "414156781",
        "group": "B"}
]

# Remove old indexes
es = XElasticIndex(conf, "customers")
indexes = es.get_indexes()
es = XElastic(conf)
res = es.delete_indexes(indexes)
assert res, 'Cleaning failed'

# Create xelastic instance for bulk indexing of the customers index
es_to = XElasticBulk(conf, 'customers', refresh='wait_for')

for item in items:
    item['created'] = int(time.time()) # Set created to the current timestamp
    # Add the current item to the bulk buffer; this sends a bulk to ES index when
    # buffer is full
    es_to.bulk_index(item)

es_to.bulk_close() # Sends the latest bulk to the ES index
