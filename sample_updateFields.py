# -*- coding: utf-8 -*-
"""
Created on Thu Mar  9 14:14:41 2023

Sample script demonstrating xelastic updateFields and updateFieldsById.
See sample_bulk for additional information

@author: juris.rats
"""
# import logging
import time
from xelastic import xelastic

# logging.basicConfig(handlers=[logging.StreamHandler()],
#                     level=logging.INFO)
#                    format=cnst.LOG_FORMAT)

conf = {
    'connection': {
        'current': 'local',
        'local': {'client': 'http://localhost:9200/'}},
    'prefix': 'ta',
    'source': 'src',
    'indexes': {
        'customers': {'stub': 'cst', 'span_type': 'm', 'date_field': 'updated'}}
}


xes = xelastic(conf, 'customers') # Create xelastic instance for customers index
xes.setUpdBody('update1', upd_fields=['phone', 'email'])
xes.setUpdBody('update2', upd_fields=['phone'], del_fields=['email'])
print(xes.upd_bodies)
print()

# update fields by query
xes.updateFields('update1', xfilter={'term': {'name': 'Jane'}},
                values = {'phone': '4242424242', 'email': 'Jane_new@xelastic.com'})

# update fields by item id  (points to the item for John)
# must specify xdate to identify the time span (and index) the item to update is located
xes.updateFieldsById('update2', xid='v1iHxoYB1hLWYRnWTupi', xdate=int(time.time()),
                values = {'phone': '66666666'})

# Print the updated index
hits, _ = xes.queryIndex()
print(hits)
