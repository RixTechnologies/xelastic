# How-to guides
## How to bulk index the data
We assume that you have already created and saved into your ES cluster the index template for sample customers index.

The script below will batch index to the customers index the data you will provide in the items list below. [See here](#how-to-configure-xelastic) for a conf sample.

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
## How to configure xelastic
Configuration data is transfered to xelastic object via conf parameter. This happens when a new xelastic object is created.
Parameter conf is a dictionary of the following structure:
```
connection:
    current: <connection name> # currently in use connection
    <connection name>:
        client: <http/https address>:9200/
        usr: [<user name>, <password>] # if authentification used
        cert: <cer file name> # if used
    <other connection name>:
        ...
prefix: <prefix for the application indexes>
source: <source name>
indexes: # a list of indexes used by the application
    # date_field must be specified if span_type != n
    <index key>: {
        stub: <index stub>,
        span_type: <d, m, q, y or n>,   # daily, monthlyl, quaterly, yearly
                                        # or not time spanned
        date_field: <date field name>,  # used for all span types but n
        shared: <True or False (default)>   # only one index is created for all
                                            # sources with source key shr
    }
    <next index key>: ...
keep: 50m # time to keep scroll batch for scroll requests
scroll_size: 200 # number of items returned in a particular scroll batch
max_rows: 999 # default maximum rows returned in Elasticsearch query
max_buckets: 999 # default maximum buckets in Elasticsearch aggregation
index_bulk: 1000 # default items in a bulk request
headers: {Content-Type: application/json} # headers to transfer in a http/https request
```
