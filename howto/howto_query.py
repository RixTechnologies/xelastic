# -*- coding: utf-8 -*-
"""
Sample script demonstrating xelastic queries and aggregation

Created on Wen Aug  9 2023

@author: juris.rats
"""
import sys, os
sys.path.append("..")
from src.xelastic import XElasticIndex

conf = {
    'connection': {'client': os.environ.get('ELASTICSEARCH_URL')},
    'prefix': 'ta',
    'source': 'src',
    'indexes': {
        'customers': {'stub': 'cst', 'span_type': 'm', 'date_field': 'created'}}
   }

es = XElasticIndex(conf, 'customers')
buckets, others = es.query_buckets('group')
print(buckets)
query = {"terms": {"name": ["Jane", "Doris"]}}
buckets = es.query_cardinality('group', query)
print(buckets)
