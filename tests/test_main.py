# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 13:06:58 2023

@author: juris.rats
"""
import os
import sys
import time

sys.path.append("..")
from src.xelastic import XElastic, XElasticIndex
from src.xelastic import XElasticScroll, XElasticBulk, XElasticUpdate

def test_main():
    """
    Test scenario:
        1. Creates the index template
        2. Indexes the data to the customers index
        3. Read in a scroll request
        4. Retrieve buckets
        5. Retrieve cardinality

        4. Update the data
        5. Count records
        6. Query the data and check
        7. Retrieve the item ids
        8. Delete an item and check
        9. Retrieves the index name
        10. Retrieve the names of the indexes available for the index key
        11. Delete the index

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
                      "group": {"type": "keyword"},
                      "created": {"type": "date", "format": "epoch_second"}
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
        'connection': {'client': os.environ.get('ELASTICSEARCH_URL')},
        'prefix': 'ta',
        'source': 'src',
        'timeout': 10,
        'indexes': {
            'customers': {'stub': 'cst', 'span_type': 'm', 'date_field': 'created'}}
       }
    
    items = [
        {"name": "John", "email": "john@xelastic.com", "phone": "12345678",
          "group": "A"},
        {"name": "Jane", "email": "john@xelastic.com", "phone": "234567811",
          "group": "A"},
        {"name": "Doris", "email": "doris@xelastic.com", "phone": "414156781",
          "group": "B"}
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
    # Step 1. Credate index template
    ###########################################################################
    template_data = {
        'customers': {
            'index': {"number_of_shards": "1", "number_of_replicas": "0"},
            'description':  "sample customer data",
            'properties': {
                "name": {"type": "keyword"},
                "email": {"type": "keyword"},
                "phone": {"type": "keyword"},
                "group": {"type": "keyword"},
                "created": {"type": "date", "format": "epoch_second"}
            }
        }
    }
    # Retrieve configuration data for all indices
    for xkey, xval in es.indexes.items():
        template_conf = template_data[xkey]
        template = es.make_template(xkey, **template_conf,
                                    refresh_interval=xval.get('ri'))

        # Create the index template
        try:
            es.set_template(xkey, template)
        except Exception as err:
            assert False, f"Step 1. Exception raised\n{err}"
        # Retrieve template
        content = es.get_template(xkey)
        assert 'index_patterns' in content, \
            f"Step 1. Invalid template {content}, no index_patterns key\n{content}"

    ###########################################################################
    # Step 2. Index the data with a bulk indexing. This creates 3 items with 
    # ids 1, 2 and 3 respectively.
    ###########################################################################
    # Create xelastic instance for bulk indexing of the customers index
    es = XElasticBulk(conf, 'customers', refresh='wait_for')
    upd_time = int(time.time())
    
    for seq, item in enumerate(items):
        item['created'] = upd_time # Set created to the current timestamp
        # Add the current item to the bulk buffer; this sends a bulk to ES index when
        # buffer is full
        es.bulk_index(item, xid=seq+1)
    
    es.bulk_close() # Sends the latest bulk to the ES index

    ###########################################################################
    # Step 3. Retrieve the data with the scroll request and check the data
    ###########################################################################
    es = XElasticScroll(conf, 'customers')
    while item := es.scroll():
        item_source = item.get('_source')
        assert item_source in items, f"Item {item_source} not found in entry data"
    
    es.scroll_close() # Removes the scroll buffer

    ###########################################################################
    # Step 4. Retrieve buckets 
    ###########################################################################
    es = XElasticIndex(conf, 'customers')
    buckets, others = es.query_buckets('group')
    assert buckets == {'A': 2, 'B': 1}, f"Step 4. Wrong buckets - {buckets}"
    assert others == 0, f"Step 4. Wrong others - {others}"

    ###########################################################################
    # Step 5. Retrieve cardinality 
    ###########################################################################
    es = XElasticIndex(conf, 'customers')
    buckets, others = es.query_cardinality('group', 'email')
    assert buckets == {'A': 2, 'B': 1}, f"Step 4. Wrong buckets - {buckets}"
    assert others == 0, f"Step 4. Wrong others - {others}"

    
    
    ###########################################################################
    # Step 4. Update data with update API
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
    # Step 5. Count the items in the index. There must be 3 records
    ###########################################################################
    es = XElasticIndex(conf, 'customers')
    count = es.count_index()
    assert count == 3, f"Step 5. Must be 3 records, counted {count}"

    ###########################################################################
    # Step 6. Query the index and check the data
    ###########################################################################
    hits, count = es.query_index()
    assert count == 3, f"Step 6. Must be 3 records, counted {count}"

    ###########################################################################
    # Step 7. Retrieve the item ids and check
    ###########################################################################
    ids = es.get_ids()
    assert len(ids) == 3, f"Step 7. Must be 3 ids, retrieved {len(ids)}"
    assert set(ids) == {'1', '2', '3'}, f"Step 7. Wrong ids - {ids}"

    ###########################################################################
    # Step 8. Delete an item
    ###########################################################################
    resp = es.delete_item(xid='3', xdate=int(time.time()), refresh='wait_for')
    assert resp, 'Item deletion failed, see log'
    count = es.count_index()
    assert count == 2, f"Step 8. Must be 2 records, counted {count}"

    ###########################################################################
    # Step 9. Retrieve the index name
    ###########################################################################
    indexes = es.get_indexes()
    assert len(indexes) == 1, f"Step 7. Must be 1 index, retrieved {len(indexes)}"
    local = time.localtime(upd_time)

    gt = '-'.join(('ta', 'cst', 'src', time.strftime("%Y", local),
                           time.strftime("%m", local)))
    assert indexes[0] == gt, f"Step 9. Wrong index name - {indexes[0]}"

    ###########################################################################
    # Step 10. Retrieve the names of the indexes available for the index key
    ###########################################################################
    res = es.get_indexes()
    assert res, 'Step 10. Retrieve index names failed'

    ###########################################################################
    # Step 11. Delete the indexes
    ###########################################################################
    es = XElastic(conf)
    res = es.delete_indexes(indexes)
    assert res, 'Step 11. Delete indexes failed'
