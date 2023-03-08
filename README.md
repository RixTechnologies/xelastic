# xelastic
Interface class for Elasticsearch. Features easy handling of scroll and bulk requests as well as handling of time spanned indexes

The xelastic class provides basic methods to handle requests to the elasticsearch indexes. To start using xelastic you should first:
* design the indexes you need in your application and create the index templates
* create the configuration dictionary
When indexing the data xelastic will automatically save it into index of the correct time span.

Visit the project pages [here](https://jurisra.github.io/xelastic)

## Quick start
* Download the code file xelastic.py
* Design the indexes of your application, e.g.
  * one index type cst with data of your customers having fields 'name', 'address' and 'created'
  * indexes will be split by month on field 'created'
  * we will use one source src
  * the prefix for our application indexes will be ta
* The above means our application indexes will have names ta-cst-src-\<yyyy-mm\> where yyyy is a year and mm is a month number
* Create index template for cst index using ta-cst* as a template pattern 
* create the sample configuration dictionary

 ```python
 conf = {'es': {
            'connection': {
                'current': 'local',
                'local': {'client': 'http://localhost:9200/'}},
            'prefix': 'ta',
            'source': 'src'
            'indexes': {
                'customers': {'stub': 'cst', 'span_type': 'm', 'date_field': 'created'}}
    }}
```

### Bulk indexing the customers index

 ```python
 from xelastic import xelastic
 es_to = xelastic(conf, 'customers')
 es_to.bulk_set()
 while True:
   # Create the dictionary item with customer data here
   # item = {'name': 'John', 'adress': 'Borneo', 'created': <e.g. current time> }
   if not item:
     break # end of the data to index
   es_to.bulk_index(item)

es_to.bulk_close()
```
Method bulk_index() adds data to the bulk and saves the bulk to the index as soon as it is full.
Method bulk_close() saves to the index the data left in the bulk at the end of the process.

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

