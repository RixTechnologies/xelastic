# -*- coding: utf-8 -*-
"""
Created on Thu Mar  9 14:14:41 2023

Sample script demonstrating xelastic updateFields and updateFieldsById.
See sample_bulk for additional information

@author: juris.rats
"""
import time
from xelastic import xelastic

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
xes.set_upd_body('update1', upd_fields=['phone', 'email'])
xes.set_upd_body('update2', upd_fields=['phone'], del_fields=['email'])
print(xes.upd_bodies)
print()

# update fields by query
xes.update_fields('update1', xfilter={'term': {'name': 'Jane'}},
               values = {'phone': '4242424242', 'email': 'Jane_new@xelastic.com'})

# update fields by item id
hits, _ = xes.query_index(body={"query": {"term": {"name": "John"}}})
xid = hits[0]["_id"] # retrieve the item id

# must specify xdate to identify the time span (and index) the item to update is located
xes.update_fields_by_id('update2', xid=xid, xdate=int(time.time()),
                values = {'phone': '66666666'}, refresh='wait_for')

# Print the updated index
hits, _ = xes.query_index()
print(hits)
