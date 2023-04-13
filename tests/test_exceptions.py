# -*- coding: utf-8 -*-
"""
Created on Thu Mar 23 13:23:44 2023

@author: juris.rats
"""
import pytest
import time
# sys.path.append("src")

from xelastic import XElastic, XElasticIndex, VersionConflictEngineException

def test_exceptions():
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
              'customers': {'stub': 'cst', 'span_type': 'm', 'date_field': 'created'}}
        }

    ###########################################################################
    # Cleaning
    ###########################################################################
    es = XElasticIndex(conf, "customers")
    indexes = es.get_indexes()
    es = XElastic(conf)
    res = es.delete_indexes(indexes)
    assert res, 'Cleaning failed'

    ###########################################################################
    # Step 1. Check the valid rest request
    ###########################################################################
    es = XElastic(conf)

    try:
        es.request('GET', endpoint='_cat/allocation')
    except:
        assert False, "Step 1. Exception raised falsely"

    ###########################################################################
    # Step 2. Check the invalid rest request
    ###########################################################################
    # es = XElastic(conf)

    # with pytest.raises(Exception):
    #     es.request('GET', endpoint='_cat/allocations')

    ###########################################################################
    # Step 3. Check the concurrency control 
    ###########################################################################
    upd = int(time.time())
    body = {"name": "John", "email": "john@xelastic.com", "phone": "12345678",
            "created": upd}
    es = XElasticIndex(conf, "customers")

    resp = es.save(body, xdate=upd, refresh='wait_for')
    ids = es.get_ids({"query": {"term": {"name": "John"}}})
    xid = ids[0]
    resp = es.get_data(xid, xdate=upd)
    seq_primary = (resp['_seq_no'], (resp['_primary_term'])) #  set valid values

    try:
        es.save(body, xid=xid, xdate=upd, seq_primary=seq_primary,
                       refresh='wait_for')
    except VersionConflictEngineException:
        assert False, "Step 2. Exception raised falsely"

    seq_primary = (resp['_seq_no'], resp['_primary_term'] + 1) #  set wrong values

    with pytest.raises(VersionConflictEngineException):
        resp = es.save(body, xid=xid, xdate=upd, seq_primary=seq_primary,
                       refresh='wait_for')

    ###########################################################################
    # Step 4. Delete the indexes
    ###########################################################################
    indexes = es.get_indexes()
    es = XElastic(conf)
    res = es.delete_indexes(indexes)
    assert res, 'Step 2. Delete indexes failed'

