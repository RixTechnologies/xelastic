# -*- coding: utf-8 -*-
"""
Sample script demonstrating xelastic bulk indexing

Assumed that an index template is created that defines 3 fields:
    customer
    street_address
    email
    phone
    updated

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
                  "updated": {"type": "date", "format": "epoch_second"}
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
import time, logging
from xelastic import xelastic

logging.basicConfig(handlers=[logging.StreamHandler()], level=logging.INFO,
    format= "%(asctime)s [%(filename)s:%(lineno)s - %(funcName)s() ] %(message)s")

conf = {
    'connection': {
        'current': 'local',
        'local': {'client': 'http://localhost:9200/'}},
    'prefix': 'ta',
    'source': 'src',
    'indexes': {
        'customers': {'stub': 'cst', 'span_type': 'm', 'date_field': 'updated'}}
   }

items = [
    {"name": "John", "email": "john@xelastic.com", "phone": "12345678"},
    {"name": "Jane", "email": "jane@xelastic.com", "phone": "234567811"},
    {"name": "Doris", "email": "doris@xelastic.com", "phone": "414156781"}
    ]

es_to = xelastic(conf, 'customers') # Create xelastic instance for customers index

es_to.bulk_set(refresh='wait_for') # Initialize the bulk indexing

for item in items:
    item['updated'] = int(time.time()) # Set updated to the current timestamp
    # Add the current item to the bulk buffer; this sends a bulk to ES index when
    # buffer is full
    es_to.bulk_index(item)

es_to.bulk_close() # Sends the latest bulk to the ES index
