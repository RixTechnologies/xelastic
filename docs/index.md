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
Use How To guides and Referenc for further guidance.
