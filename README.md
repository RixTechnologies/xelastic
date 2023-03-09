# xelastic
Interface class for Elasticsearch. Features
* easy handling of scroll and bulk requests
* easy updates
* handling of time spanned indexes

The xelastic class provides basic methods to handle requests to the elasticsearch indexes. To start using xelastic you should first:
* design the indexes you need in your application and create the index templates
* create the configuration dictionary

When indexing the data xelastic will automatically save it into index of the correct time span.

Visit the project pages [here](https://jurisra.github.io/xelastic)

## Quick start
* Download the code file xelastic.py
* Design the indexes of your application, e.g.
  * one index type cst with data of your customers having fields 'name', 'street_address', 'email', 'phone' and 'updated'
  * indexes will be split by month on field 'updated'
  * we will use one source src
  * the prefix for our application indexes will be ta
* The above means our application indexes will have names ta-cst-src-\<yyyy-mm\> where yyyy is a year and mm is a month number; xelastic will take care new data is routed to respective monthly index
* Create index template for cst index using ta-cst* as a template pattern; this will ensure the monthly indexes are created automatically when necessary
* create the sample configuration dictionary

 ```python
 conf = {'es': {
            'connection': {
                'current': 'local',
                'local': {'client': 'http://localhost:9200/'}},
            'prefix': 'ta',
            'source': 'src',
            'indexes': {
                'customers': {'stub': 'cst', 'span_type': 'm', 'date_field': 'updated'}}
    }}
```

### Bulk indexing the customers index

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

### Retrieving the data with scroll

```python
from xelastic import xelastic
es_from = xelastic(conf, 'customers')
es_from.scroll_set()
while item != es_from.scroll():
  # Handle the item data

es_from.scroll_close()
```
Method scroll() takes next item from the scroll buffer and retrieves the next scroll batch when
the buffer empty.
Method scroll_close() removes the scroll request.

