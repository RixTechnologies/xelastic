# xelastic
Interface class for Elasticsearch. Features easy handling of scroll and bulk requests as well as handling of time spanned indexes

The xelastic class provides basic methods to handle requests to the elasticsearch indexes. To start using xelastic you should first:
* design the indexes you need in your application and create the index templates
* create the configuration dictionary

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

### Index the customers index using bulk indexing
