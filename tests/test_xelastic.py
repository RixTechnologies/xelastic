# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 13:06:58 2023

@author: juris.rats
"""
import sys
sys.path.append("src")

from xelastic import xelastic

def test_xelastic():
    """
    Connection to the Elasticsearch must be active
    """
    conf = {
          'connection': {
              'current': 'local',
              'local': {'client': 'http://localhost:9200/'}},
          'prefix': 'ta',
          'source': 'src',
          'indexes': {
              'customers': {'stub': 'cst', 'span_type': 'm', 'date_field': 'updated'}}
        }

    xes = xelastic(conf, "customers")
    tstamp = 1678792737 # timestamp for 2023-03-14
    index_name = xes.index_name(tstamp)
    assert index_name == 'ta-cst-src-2023-03', \
        f"Index name must be 'ta-cst-src-2023-03', is {index_name}"
