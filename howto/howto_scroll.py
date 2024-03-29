# -*- coding: utf-8 -*-
"""
Sample script demonstrating xelastic scroll interface. See sample_bulk for
additional information

Created on Thu Mar  9 15:26:41 2023

@author: juris.rats
"""
import sys, os
sys.path.append("..")
from src.xelastic import XElasticScroll

conf = {
    'connection': {'client': os.environ.get('ELASTICSEARCH_URL')},
    'prefix': 'ta',
    'source': 'src',
    'indexes': {
        'customers': {'stub': 'cst', 'span_type': 'm', 'date_field': 'created'}}
   }

es_from = XElasticScroll(conf, 'customers') # Create xelastic instance for customers index
# Retrieve an item from the scroll batch. Retrieve next batch if the current one
# is empty
while item := es_from.scroll():
  print(item)

es_from.scroll_close() # Removes the scroll buffer
