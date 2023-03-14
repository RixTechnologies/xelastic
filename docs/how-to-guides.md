# How-to guides
## How to set xelastic up
* Download the code file xelastic.py
* Design the indexes of your application, e.g.
  * one index type cst with data of your customers having fields 'name', 'street_address', 'email', 'phone' and 'updated'
  * indexes will be split by month on field 'updated'
  * we will use one source src
  * the prefix for our application indexes will be ta
* The above means our application indexes will have names ta-cst-src-\<yyyy-mm\> where yyyy is a year and mm is a month number; xelastic will take care new data is routed to respective monthly index
* Create index template for cst index using ta-cst* as a template pattern; this will ensure the monthly indexes are created automatically when necessary
* create the configuration dictionary (reference to full description of config dictionary)

```python
conf = {
	'connection': {
		'current': 'local',
		'local': {'client': 'http://localhost:9200/'}},
	'prefix': 'ta',
	'source': 'src',
	'indexes': {
		'customers': {'stub': 'cst', 'span_type': 'm', 'date_field': 'updated'}}
}
```

## How to bulk index the data
We assume that you have already created and saved into your ES cluster the index template for sample customers index.
You can use here the sample. [See here](#how-to-set-xelastic-up) for setup and conf sample.

The script below will batch index to the customers index the data you will provide in the items list below.

```python
import time
from xelastic import xelastic

items = [{"name": "John", "email": "john@xelastic.com", "phone": "12345678"}, ...]

es_to = xelastic(conf, 'customers') # Create xelastic instance for customers index
es_to.bulk_set() # Initialize the bulk indexing
for item in items:
    item['updated'] = int(time.time()) # Set updated to the current timestamp
    # Add the current item to the bulk buffer; this sends a bulk to ES index when
    # buffer is full
    es_to.bulk_index(item)

es_to.bulk_close() # Sends the latest bulk to the ES index
```

## How to retrieve data with scroll
You should use index template and [conf dictionary](#how-to-set-xelastic-up) as well as fill index with some data before to use the script below. You may use [bulk index script](#how-to-bulk-index-the-data) to fill the index.

```python
from xelastic import xelastic
es_from = xelastic(conf, 'customers') # Create xelastic instance for customers index
es_from.scroll_set() # Initialize the scroll requests
# Retrieve an item from the scroll batch. Retrieve next batch if the current one
# is empty
while item := es_from.scroll():
  print(item)

es_from.scroll_close() # Removes the scroll buffer
```
## How to update the fields by query and by ID
 You should use index template and [conf dictionary](#how-to-set-xelastic-up) as well as use script described in (link??) to create data before to use the script below.

```python
import time
from xelastic import xelastic

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
```