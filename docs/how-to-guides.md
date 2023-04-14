# How-to guides
## How to bulk index the data
We assume that you have already created and saved into your ES cluster the index template for sample customers index.

The script below will batch index to the customers index the data you will provide in the items list below.
See [here](reference.md#src.xelastic.XElastic.__init__) for the description of the conf parameter.

```python
import time
from xelastic import XElasticBulk

items = [{"name": "John", "email": "john@xelastic.com", "phone": "12345678"}, ...]

es_to = XElasticBulk(conf, 'customers') # Create xelastic instance for customers index
for item in items:
    item['created'] = int(time.time()) # Set created to the current timestamp
    # Add the current item to the bulk buffer; this sends a bulk to ES index when
    # buffer is full
    es_to.bulk_index(item)

es_to.bulk_close() # Sends the latest bulk to the ES index
```

## How to retrieve data with scroll
You should use index template and [conf dictionary](#how-to-configure-xelastic) as well as fill index with some data before to use the script below. You may use [bulk index script](#how-to-bulk-index-the-data) to fill the index.

```python
from xelastic import XElasticScroll
es_from = XElasticScroll(conf, 'customers') # Create xelastic instance for customers index
# Retrieve an item from the scroll batch. Retrieve next batch if the current one
# is empty
while item := es_from.scroll():
  print(item)

es_from.scroll_close() # Removes the scroll buffer
```
## How to update the fields by query and by ID
 You should use index template and [conf dictionary](#how-to-configure-xelastic) as well as use script described in (link??) to create data before to use the script below.

```python
import time
from xelastic import XElasticUpdate

xes = XElasticUpdate(conf, 'customers') # Create xelastic instance for customers index
xes.setUpdBody('update1', upd_fields=['phone', 'email'])
xes.setUpdBody('update2', upd_fields=['phone'], del_fields=['email'])
print(xes.upd_bodies)
print()

# update fields by query
xes.update_fields('update1', xfilter={'term': {'name': 'Jane'}},
               values = {'phone': '4242424242', 'email': 'Jane_new@xelastic.com'})

# update fields by item id
xid = xes.get_ids(body={"query": {"term": {"name": "John"}}})[0]

# must specify xdate to identify the time span (and index) the item to update is located
xes.update_fields_by_id('update2', xid=xid, xdate=int(time.time()),
                values = {'phone': '66666666'}, refresh='wait_for')

# Print the updated index
hits, _ = xes.query_index()
print(hits)
```
## How to handle exceptions
All xelastic methods that do not provide specific exception handling just raise the catched exceptions to leave handling for the caller.
Exceptions are:

* resource not found (exception not raised, None returned)
* VersionConflictEngineException - the exception raised when version conflict occurs while trying to update index ([see How to handle update conflicts](#how-to-handle-update-conflicts))

## How to handle update conflicts
Elasticsearch uses a versioning system to manage conflicts that can occur when multiple users or processes
try to modify the same document at the same time. A unique version of the document is identified by values of 
metafields _seq_no and _primary_term. The application has to check the values of these fields
when updating the document to ensure that it updates the right version of the document.

A sample workflow of the document update:

* read the document with get_data and remember values of _seq_no and _primary_term
* make updates to the document
* save the document with save method and transfer the remembered _seq_no / _primary_term
values in seq_primary parameter
* catch VersionConflictEngineException to identify a version conflict 

## How to handle logging
xelastic uses python basic logging. You may configure logging to match your preferences.
E.g.:
```
import logging
logging.basicConfig(
    handlers=[logging.StreamHandler()],
    level=logging.INFO,
    format= "%(asctime)s [%(filename)s:%(lineno)s - %(funcName)s() ] %(message)s")
```
