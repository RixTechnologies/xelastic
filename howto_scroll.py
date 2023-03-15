# -*- coding: utf-8 -*-
"""
Sample script demonstrating xelastic scroll interface. See sample_bulk for
additional information

Created on Thu Mar  9 15:26:41 2023

@author: juris.rats
"""
from src.xelastic import XElastic

conf = {
    'connection': {
        'current': 'local',
        'local': {'client': 'http://localhost:9200/'}},
    'prefix': 'ta',
    'source': 'src',
    'indexes': {
        'customers': {'stub': 'cst', 'span_type': 'm', 'date_field': 'updated'}}
   }

es_from = XElastic(conf, 'customers') # Create xelastic instance for customers index
es_from.scroll_set() # Initialize the scroll requests
# Retrieve an item from the scroll batch. Retrieve next batch if the current one
# is empty
while item := es_from.scroll():
  print(item)

es_from.scroll_close() # Removes the scroll buffer
