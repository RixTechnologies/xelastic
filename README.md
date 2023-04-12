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
