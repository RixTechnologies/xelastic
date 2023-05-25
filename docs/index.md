## How to install and set up xelastic
### Install xelastic
Go to Releases, download the whl file and install xelastic with a command
```
pip install <path to the whl file on your computer>
```
### Design the indexes of your application
The sample configuration we use here is

* a single customers index having fields *name*, *street_address*, *email*, *phone* and *created*
* indexes will be split by month on field *created*
* we use a single source *src*
* the prefix for our application indexes is *ta*

The above means our application indexes will have names ta-cst-src-&lt;yyyy-mm&gt; where yyyy is a year and mm is a month number; xelastic will take care new data is routed to respective monthly index

### Create index template
Create the index template for cst index using ta-cst* as a template pattern;
this will ensure the monthly indexes are created automatically when necessary
with the mappings and settings specified;
### Create the configuration dictionary

```python
conf = {
    'connection': {'client': os.environ('ELASTICSEARCH_URL')},
	'prefix': 'ta',
	'source': 'src',
	'indexes': {
		'customers': {'stub': 'cst', 'span_type': 'm', 'date_field': 'created'}}
}
```
[See here](reference.md#src.xelastic.XElastic.__init__) for full description of the configuration dictionary. Use How-To guides and Reference for further assistance.
