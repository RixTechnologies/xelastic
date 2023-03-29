# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 13:06:58 2023

@author: juris.rats
"""
import time
import sys

from xelastic import XElastic, XElasticIndex
from xelastic import XElasticScroll, XElasticBulk, XElasticUpdate

def test_bulk_scroll_update_delete():
    """
    Test scenario:
        1. Indexes the data to the customers index (template must be created)
        2. Read in a scroll request
        3. Update the data
        4. Count records
        5. Query the data and check
        6. Retrieve the item ids
        7. Delete an item and check
        8. Retrieves the index name
        9. Retrieve the names of the indexes available for the index key
        10. Delete the index

    Template to create
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
    """
    conf = {
        'connection': {
            'current': 'local',
            'local': {'client': 'http://localhost:9200/'}},
        'prefix': 'ta',
        'source': 'src',
        'timeout': 10,
        'indexes': {
            'customers': {'stub': 'cst', 'span_type': 'm', 'date_field': 'updated'}}
       }
    
    items = [
        {"name": "John", "email": "john@xelastic.com", "phone": "12345678"},
        {"name": "Jane", "email": "jane@xelastic.com", "phone": "234567811"},
        {"name": "Doris", "email": "doris@xelastic.com", "phone": "414156781"}
        ]
    
    ###########################################################################
    # Cleaning
    ###########################################################################
    es = XElasticIndex(conf, "customers")
    indexes = es.get_indexes()
    es = XElastic(conf)
    res = es.delete_indexes(indexes)
    assert res, 'Cleaning failed'

    ###########################################################################
    # Step 1. Index the data with a bulk indexing. This creates 3 items with 
    # ids 1, 2 and 3 respectively.
    ###########################################################################
    # Create xelastic instance for bulk indexing of the customers index
    es = XElasticBulk(conf, 'customers', refresh='wait_for')
    upd_time = int(time.time())
    
    for seq, item in enumerate(items):
        item['updated'] = upd_time # Set updated to the current timestamp
        # Add the current item to the bulk buffer; this sends a bulk to ES index when
        # buffer is full
        es.bulk_index(item, xid=seq+1)
    
    es.bulk_close() # Sends the latest bulk to the ES index

    ###########################################################################
    # Step 2. Retrieve the data with the scroll request and check the data
    ###########################################################################
    es = XElasticScroll(conf, 'customers')
    while item := es.scroll():
        item_source = item.get('_source')
        assert item_source in items, f"Item {item_source} not found in entry data"
    
    es.scroll_close() # Removes the scroll buffer

    sys.exit()
    ###########################################################################
    # Step 3. Update data with update API
    ###########################################################################
    es = XElasticUpdate(conf, 'customers') # Create xelastic instance for customers index
    es.set_upd_body('update1', upd_fields=['phone', 'email'])
    es.set_upd_body('update2', upd_fields=['phone'], del_fields=['email'])
    
    # update fields by query
    es.update_fields('update1', xfilter={'term': {'name': 'Jane'}},
                   values = {'phone': '4242424242', 'email': 'Jane_new@xelastic.com'})
    
    # update fields by item id
    xid = es.get_ids(body={"query": {"term": {"name": "John"}}})[0]
    
    # must specify xdate to identify the time span (and index) the item to update is located
    es.update_fields_by_id('update2', xid=xid, xdate=int(time.time()),
                    values = {'phone': '66666666'}, refresh='wait_for')

    ###########################################################################
    # Step 4. Count the items in the index. There must be 3 records
    ###########################################################################
    es = XElasticIndex(conf, 'customers')
    count = es.count_index()
    assert count == 3, f"Step 4. Must be 3 records, counted {count}"

    ###########################################################################
    # Step 5. Query the index and check the data
    ###########################################################################
    hits, count = es.query_index()
    assert count == 3, f"Step 5. Must be 3 records, counted {count}"

    ###########################################################################
    # Step 6. Retrieve the item ids and check
    ###########################################################################
    ids = es.get_ids()
    assert len(ids) == 3, f"Step 6. Must be 3 ids, retrieved {len(ids)}"
    assert set(ids) == {'1', '2', '3'}, f"Step 6. Wrong ids - {ids}"

    ###########################################################################
    # Step 7. Delete an item
    ###########################################################################
    resp = es.delete_item(xid='3', xdate=int(time.time()), refresh='wait_for')
    assert resp, 'Item deletion failed, see log'
    count = es.count_index()
    assert count == 2, f"Step 7. Must be 2 records, counted {count}"

    ###########################################################################
    # Step 8. Retrieve the index name
    ###########################################################################
    indexes = es.get_indexes()
    assert len(indexes) == 1, f"Step 7. Must be 1 index, retrieved {len(indexes)}"
    local = time.localtime(upd_time)

    gt = '-'.join(('ta', 'cst', 'src', time.strftime("%Y", local),
                           time.strftime("%m", local)))
    assert indexes[0] == gt, f"Step 8. Wrong index name - {indexes[0]}"

    ###########################################################################
    # Step 9. Retrieve the names of the indexes available for the index key
    ###########################################################################
    res = es.get_indexes()
    assert res, 'Step 9. Retrieve index names failed'

    ###########################################################################
    # Step 10. Delete the indexes
    ###########################################################################
    es = XElastic(conf)
    res = es.delete_indexes(indexes)
    assert res, 'Step 10. Delete indexes failed'
