# -*- coding: utf-8 -*-
"""
Created on Thu Mar 23 13:23:44 2023

@author: juris.rats
"""
import pytest
import sys, logging, time
sys.path.append("src")

from xelastic import XElastic, XElasticIndex, VersionConflictEngineException

def test_concurrency():
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

    logging.basicConfig(handlers=[logging.StreamHandler()],
            level=logging.info,
            format="%(asctime)s [%(filename)s:%(lineno)s - %(funcName)s() ] %(message)s")

    ###########################################################################
    # Cleaning
    ###########################################################################
    es = XElasticIndex(conf, "customers")
    indexes = es.get_indexes()
    es = XElastic(conf)
    res = es.delete_indexes(indexes)
    assert res, 'Cleaning failed'

    ###########################################################################
    # Step 1. Check the concurrency control 
    ###########################################################################
    upd = int(time.time())
    body = {"name": "John", "email": "john@xelastic.com", "phone": "12345678",
            "updated": upd}
    es = XElasticIndex(conf, "customers")

    resp = es.save(body, xdate=upd, refresh='wait_for')
    ids = es.get_ids({"query": {"term": {"name": "John"}}})
    xid = ids[0]
    resp = es.get_data(xid, xdate=upd)
    seq_primary = (resp['_seq_no'], (resp['_primary_term'] )) #  set wrong values

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
    # Step 2. Delete the indexes
    ###########################################################################
    indexes = es.get_indexes()
    es = XElastic(conf)
    res = es.delete_indexes(indexes)
    assert res, 'Step 2. Delete indexes failed'

