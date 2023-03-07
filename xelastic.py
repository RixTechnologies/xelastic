# -*- coding: utf-8 -*-
"""
Created on Wed Apr 14 10:56:39 2021

@author: juris.rats
"""
import requests
from requests.auth import HTTPBasicAuth
import json, copy, logging, urllib
import time
from datetime import datetime, timedelta
# Union, Set, List, Tuple, Collection, Any, Dict, Optional, NoReturn
from typing import Tuple, Any, Dict, Optional

SPAN_ALL = 'all'        # span name for spantype == 'n'

class xelastic():
    """
    Elasticsearch interface class. Provides easy handling of scroll and bulk
    requests as well as handling of time split indexes.

    External methods
        setSource
        indexName
        start
        end
        request
        getIndexes
        deleteIndexes
        getData
        getDataX
        save
        existsIndex
        countIndex
        queryIndex
        aggIndex
        queryBuckets
        queryCardinality
        setRefresh
        createTermFilter
        setUpdBody
        updateFields
        updateFieldsById
        ========== Scroll API
        scroll_set
        scroll_total
        scroll
        scroll_delete
        ========== Bulk API
        bulk_set
        bulk_index
        bulk_close
        ==========
        termvectors
        mlt

    The xelastic class uses index names of the format: prefix-stub-source-span
    Where
        prefix is shared by all indexes of the application
        stub identifies indexes of a particular type
        source identifies a particular data set
        span identifies a particular time period (span)

    prefix-stub is used in index templates thus these are indexes with identic
    settings and mappings

    conf is the dictionary of the following form
        es:
            connection:
                current: <name of the selected connection>
                <Name of the connection 1>:
                    client: <client url>
                    cert: <path to the certificte file>, optional
                    usr: [<user name>, <password>] for authentification, optional
                <Name of the connection 2>:
                ... 
            prefix: <prefix of the application index names>
            source: <application default source ID>
            indexes:
                <index key 1>:
                    stub: <stub>
                    span_type: <valid span type id: d, m, q, y or n>
                    date_field: <date field to split the indexes on>, must be set
                            for all span types except n
                    shared: <True or False>, specifies the index shared for all
                            sources, default False, optional
                <index key 2>
                ...
            keep: <time to keep scroll batch> defaults to '10s'
            scroll_size: <amoun of the scroll batch in bytes> defaults to 100
            max_buckets: <maximum buckets in es aggregation>, defaults to 99
            index_bulk: <number of rows in an index bulk>, defaults to 1000
            high: <Maximum allowed used disk space %>, defaults to 90%, execution
                    is aborted if the used space is higher
            headers: <headers for the http request>,
                    defaults to {Content-Type: application/json}
    

    Response codes: 200 (ok), 201 (created succesfully), 400 (bad request), 401 (not authorised)
    """

    def __init__(self, conf: dict, index_key=None, terms=None, mode=None):
        """
        Initializes the instance:
            conf/es/source - third part of the index names, denotes a
                group of indices
            conf/es - configuration dictionary for the Elasticsearch connetcion
            index_key - the index key for the instance
            terms - terms dictionary of form {key1: value1, key2: value2, ...}
                to handle subset of the data
            mode - may set mode for all requests for the current class instance
        """
        self.mode = mode

        esconf = conf['es']

        self.source = esconf['source']
        if index_key:
            assert index_key in esconf['indexes'].keys(), \
                f"Nepareiza indeksa atslēga {index_key}"

        # Handle index configuration
        index_conf = esconf['indexes'][index_key]
        self.index_key = index_key
        self.terms = terms
        self.prefix = esconf['prefix']

        self.span_type = index_conf.get('span_type', 'n')
        assert self.span_type in ('n','y','q','m','d'),\
            f"Wrong index span type {self.span_type}"
        self.date_field = index_conf.get('date_field')
        assert self.span_type == 'n' or self.date_field, \
            f"Date field must be set for index of span type {self.span_type}"
        self.stub = index_conf['stub']
        self.span_type = index_conf['span_type']

        # Retrieving specified connection and environment keys
        ckey = esconf['connection']['current']
        self.es_client = esconf['connection'][ckey]['client']
        self.cert = esconf['connection'][ckey].get('cert', False)
        usr = esconf['connection'][ckey].get('usr')

        self.auth = None if not usr else HTTPBasicAuth(*usr)
        self.headers = esconf.get('headers',
                                  {"Content-Type": "application/json"})
        self.keep = esconf.get('keep', '10s')
        self.bulk_max = esconf.get('index_bulk', 1000)
        self.max_buckets = esconf.get('max_buckets', 99)
        self.scroll_size = esconf.get('scroll_size', 100)
        self.upd_bodies: Dict[str, Dict[str, Any]] = {} # placeholder for update scripts
        
        # Retrieve the Elasticsearch version
        resp = self.request('GET', use_index_key=False, mode=mode).json()
        self.es_version = int(resp['version']['number'].split('.')[0])
        self.es_doc = '/_doc/' if self.es_version < 7 else '/'
        self.bulkcurr = None # indicates that bulk indexing is not initialized
        
        self._usageOk(esconf.get('high', 90), mode=mode) # Aborts if disk usage too high
        self.setSource(self.source)
        
    def _usageOk(self, high, mode=None):
        """
        Aborts if disk usage is more as configuration es/high value
        """
        endpoint = "_cat/allocation"
        resp = self.request(mode=mode,
            command='GET', endpoint=endpoint, use_index_key=False)
        usage = int(resp.text.split()[5])
        assert usage <= high, f"Disk usage {usage}% - exceeds allowed {high}%"

    def setSource(self, source):
        """
        Changes the source
        """
        self.source = source

    def indexName(self, epoch: int = None) -> Optional[str]:
        """
        Get index name for the stub, source and epoch.

        If span_type == 'n' SPAN_ALL is used as span in the index name
        Otherwise
            if epoch set span is calculated from the epoch
            else * is used for span (all spans addressed)
        """
        if self.span_type == 'n':
            span = SPAN_ALL
        elif not epoch:
            span = '*'
        else:
            ts = time.localtime(epoch)
            try:
                span = {
                    'y': time.strftime("%Y",ts),
                    'q': '-'.join((time.strftime("%Y",ts), str(int(time.strftime("%m",ts)) // 3 + 1))),
                    'm': '-'.join((time.strftime("%Y",ts), time.strftime("%m",ts))),
                    'd': '-'.join((time.strftime("%Y",ts), time.strftime("%m",ts), time.strftime("%d",ts)))
                }[self.span_type]
            except KeyError:
                return None
        return '-'.join((self.prefix, self.stub, self.source, span))

    def start(self, span: str) -> Optional[int]:
        """
        Returns the start epoch of the span
        """
        if self.spanType=='y':
            return int(datetime(int(span),1,1).timestamp())
        elif self.span_type=='q':
            year, quarter = span.split('-')
            return int(datetime(int(year), int(quarter) * 3 - 2, 1).timestamp())
        elif self.span_type=='m':
            year, month = span.split('-')
            return int(datetime(int(year), int(month), 1).timestamp())
        elif self.span_type=='d':
            year, month, day = span.split('-')
            return int(datetime(int(year), int(month), int(day)).timestamp())
        else:
            return None

    def end(self, span: str) -> Optional[int]:
        """
        Returns the end epoch of the span
        """
        if self.span_type=='y':
            return int(datetime(int(span)+1, 1, 1).timestamp())
        elif self.span_type=='q':
            year, quarter = span.split('-')
            year, month = self._next_month(int(year), int(quarter) * 3)
            return int(datetime(year, month, 1).timestamp())
        elif self.span_type=='m':
            year, month = span.split('-')
            year, month = self._next_month(int(year), int(month))
            return int(datetime(year, month, 1).timestamp())
        elif self.span_type=='d':
            year, month, day = span.split('-')
            return int((datetime(int(year), int(month), int(day)) +
                       timedelta(days=1)).timestamp())
        else:
            return None

    def _next_month(self, xyear: int, xmonth: int) -> Tuple[int, int]:
        """
        Returns year and month for the next month
        """
        return (xyear, xmonth + 1) if xmonth < 12 else (xyear + 1, 1)

    def _make_params(self, url:str, params:dict)->str:
        """
        Makes parameter string of form ?par1&par2 ...
        """
        return '?'.join((url, urllib.parse.urlencode(params)))

    def request(self, command='POST', endpoint='', seq_primary=None, refresh=None,
                body:dict=None, xdate=None, use_index_key=True, mode=None):
        """
        Wrapper on _request_json. Converts dictionary <body> to json string
        
        NB!! Does not use self.filter
        """
        data = json.dumps(body) if body else None
        return self._request_json(command, endpoint, seq_primary, refresh, data,
                                  xdate, use_index_key, mode)

    def _request_json(self, command='POST', endpoint=None, seq_primary=None,
                      refresh=None, body=None, xdate=None, use_index_key=True,
                      mode=None):
        """
        Wrapper to the requests method request. Body is a json string. 
        In most cases called from request method. Directly used e.g. for bulk
        indexing.
        
        If use_index_key set to False, does not use index name for es request
        
        Values for refresh:
            not set or False - no refresh actions
            wait for - waits for the refresh to proceed
            empty string or true (not recommended) - immedially refreh the
                relevant index shards

        Values for mode: 
            None (run) to run silently
            f (fake) to log parameters without running the request
            v (or any other value - verbose) to run and log data
        Mode might be set by the methods parameter or by the instance variable
        If both set parameter has a precedence.
        
        Returns requests object resp:
           status_code - http status code
           text - returned result as a json or text
           json() - converting the returned result to python list, use
              only for the requests returning json, do not use for _cat
        """
        if not mode: mode = self.mode
        url = self.es_client
        if self.index_key and use_index_key:
            url += self.indexName(xdate) + '/'
        if endpoint:
            url += endpoint
        params = {}
        if seq_primary:
            params['if_seq_no'] = seq_primary[0]
            params['if_primary_term'] = seq_primary[1]
        if refresh:
            params['refresh'] = refresh
        if params: # Add url parameters if specified
            #req = requests.get(url, params=params)
            #url = req.url
            url = self._make_params(url, params)

        if mode:
            logger = logging.getLogger(__name__)
            logger.info(f"command {command}, index_key {self.index_key}"
                        f" url {url} body {body}")
        if mode == 'f':
            # execute dummy request
            return requests.request('GET', self.es_client,
                            auth = self.auth,
                            verify = self.cert,
                            headers = self.headers)
        else:
            return requests.request(command, url,
                                    data = body,
                                    auth = self.auth,
                                    verify = self.cert,
                                    headers = self.headers)

    def getIndexes(self):
        """
        Return a list of existing index names for the index key
        """
        resp = self.request(command='GET', endpoint='_settings')
        if resp.status_code == 404:
            return []   # Mo indexes found, return empty list
        return list(resp.json().keys())

    def deleteIndexes(self, indexes, mode=None):
        """
        Deletes indexes of the list <indexes>
        Returns True if all indexes deleted succesfully
        """
        success = True
        for index in indexes:
            # set index name directly to handle indexes with time spans -
            # here indexes have to be deleted one by one
            resp = self.request(command="DELETE", endpoint=index,
                                use_index_key=False, mode=mode)
            if not resp.json().get('acknowledged'):
                logger = logging.getLogger(__name__)
                logger.error(resp.text)
                success = False
        return success
            
    def getData(self, xid, mode=None):
        """
        Retrieve data for <xid> from the current index
        
        Returns the item data (_source) or None if item with id <xid> not found
        """
        resp = self.getDataX(xid, mode)
        return resp.get('_source') if resp else None

    def getDataX(self, xid, mode=None):
        """
        Retrieve data for <xid> from the current index
        
        Returns the full json (_source and metadata) or None if item with id <xid> not found
        """
        endpoint = '/'.join(('_doc', xid))
        resp = self.request(command='GET', endpoint=endpoint, mode=mode)
        if resp.status_code != 200:
            logger = logging.getLogger(__name__)
            logger.error(resp.text, stack_info=True)
            return None
        return resp.json()

    def save(self, body, xid=None, seq_primary=None, refresh=None, xdate=None, mode=None):
        """
        Index an item
        Adds to the data body self.terms to save the data of the terms fields
        
        Returns id of the created item or None on failure
        """
        if self.terms:
            for key, val in self.terms.items():
                body[key] = val
        endpoint = '_doc/'
        if xid:
            endpoint += xid

        resp = self.request(endpoint=endpoint, seq_primary=seq_primary,
                            refresh=refresh, body=body,
                            xdate=xdate,  mode=mode)
        if resp.status_code != 201: # resource created
            logger = logging.getLogger(__name__)
            logger.error(resp.text, stack_info=True)
            return None
        return resp.json()['_id']
        
    def existsIndex(self, mode=None):
        """
        Returns True if index exists, false otherwise
        """
        index_name = self.indexName()
        #index_name = 'xxx'
        endpoint = f"_cat/indices/{index_name}?h=s,idx&format=json"
        resp = self.request(mode=mode,
            command='GET', endpoint=endpoint, use_index_key=False)
        status = resp.status_code
        assert status in (200, 404), f"existsIndex returned status {resp.text}"
        return (status == 200)

    def countIndex(self, body=None, mode=None):
        """
        Counts the items in index_key according to the criteria in body
        Adds self.filter if set
        """
        resp = self.request(endpoint="_count", body=self._addFilter(body),
                            mode=mode)
        if resp.status_code == 200:
            return resp.json()['count']
        else:
            logger = logging.getLogger(__name__)
            logger.error(resp.text, stack_info=True)
            return 0

    def queryIndex(self, body, mode=None):
        """
        Returns list of requested rows and total count of matching rows
        If no results - returns empty list and 0
        When error returns empty list and the error type (string)
        Adds self.filter if set
        """
        resp = self.request(endpoint="_search", body=self._addFilter(body),
                            mode=mode)
        if resp.status_code == 200:
            hits = resp.json()['hits']
            return hits['hits'], hits['total']['value']
        if resp.status_code == 404:
            # Index not found
            pass
        else:
            logger = logging.getLogger(__name__)
            logger.error(resp.text, stack_info=True)
        return [], 0

    def aggIndex(self, body:dict, mode:str=None) -> Optional[list]:
        """
        Returns list of returned rows of aggregated values and count of documents in smaller groups
        Adds self.filter if set
        """
        resp = self.request(endpoint="_search", body=self._addFilter(body),
                            mode=mode)
        if resp.status_code == 200:
            # If no data returned response dictionary has no <aggregations> item
            return  resp.json().get('aggregations', [])
        if resp.status_code == 404:
            # Index not found
            return []
        # request failed
        logger = logging.getLogger(__name__)
        logger.error(resp.text, stack_info=True)
        return None

    def queryBuckets(self, field: str, query: Dict[str, Any] =None,
                     max_buckets : int =None, quiet: bool =False, mode=None
                     ) -> Tuple[Dict[str, int], int]:
        """
        Retrieves the buckets data on <field>.
        
        Returns a dictionary of form key: doc count, and the number of other
        documents (not aggregated)
        
        If quiet==False logs sum_other_doc_count
        """
        mbuckets = max_buckets if max_buckets else self.max_buckets
        body = {"size": 0,
                "aggs": {"agg": {"terms": {"field": field, "size": mbuckets}}}}
        if query:
            body['query'] = query
        aggs = self.aggIndex(body, mode)
        #print(body, index_key, aggs)
        if aggs:
            buckets = aggs['agg']['buckets']
            others = aggs['agg'].get('sum_other_doc_count',0)
        else:
            return {}, 0
        if others > 0 and not quiet:
            logger = logging.getLogger(__name__)
            logger.info(f"{others} items not aggregated: "
                         f"{field} {self.index_key} {mbuckets}")
        xbuckets = {x['key']: x['doc_count'] for x in buckets}
        return xbuckets, others     

    def queryCardinality(self, field: str) -> int:
        """
        Retrieve the number of unique values of <field> (cardniality)
        Adds self.filter if set
        """
        body = {"size": 0,
          "aggs": {
            "agg": {
              "cardinality": {
                "field": field
        }}}}
        resp = self.request(endpoint='_search', body=self._addFilter(body))
        assert resp.status_code == 200, resp.text
        
        return resp.json()["aggregations"]["agg"]["value"]

    def setRefresh(self, period='1s', mode=None):
        """
        Sets refresh interval for the index xkey to period.
        Period has form 'xxxs' where xxx is number of seconds
        """
        body = {"index": {"refresh_interval": period}}
        resp = self.request(command='PUT', endpoint='_settings', body=body,
                            mode=mode)
        ok = True
        if resp.status_code != 200:
            ok = False
            logger = logging.getLogger(__name__)
            logger.info(f"Status {resp.status_code} _settings " 
                        f"{body}\n {resp.text}")
        return ok

    def createTermFilter(self, terms:dict):
        """
        Creates terms filter ([{"term": {<field>: <value>}}, ...]) from the
        <terms> dictionary
        """
        return [{"term": {key, val}} for key, val in terms]

    def _addFilter(self, body=None, mode=None):
        """
        If self.terms set creates new filter merging body/query and self.terms

        Uses a copy of the body parameter to avoid changing the parameter value

        Otherwise just returns body
        
        NB. Query transfered in the body parameter must be a list!!
        """
        
        if not self.terms:
            return {} if body is None else body

        xbody = {} if body is None else copy.deepcopy(body)
        xfilter = self.createTermFilter(self.terms)
        query = xbody.get('query')
        if not query:
            # Set self.filter as a query
            xbody['query'] = xfilter
        elif query and 'bool' in query:
            # the body query is a bool query
            if 'filter' not in query['bool']:
                xbody['query']['bool']['filter'] = []
            xbody['query']['bool']['filter'].append(xfilter)
        else:
            # body query is a simple query, transform to bool query
            # Assumed that a simple query should be converted to must query
            xbody['query'] = {'bool': {
                'filter': [xfilter],
                'must': [query]
            }}
        return xbody

    def setUpdBody(self, name: str, upd_fields: list = None, del_fields: list = None, mode=None):
        """
        name - name of th update script
        upd_fields - fields to update
        del_fields - fields to remove
        mode not used
        """
        
        self.upd_bodies[name] = {"script": {
                "source": self._updFields(upd_fields, del_fields),
                "lang": "painless"}}

    def updateFields(self, name: str, xfilter: dict, values: dict=None,
                     xdate=None, refresh=None, mode=None):
        """
        Update / delete fields for items filtered by xfilter (update by query)
        If update body <name> has update fields, <values> must be specified

        Returns number of updated items if updates successful, -1 otherwise
        Logs errors on failure
        {"took": ?, "timed_out": false, "total": ?, "updated": ?, ...}

        """
        body = self.upd_bodies[name]
        body['query'] = xfilter
        if values:
            body['script']['params'] = values
        endpoint = '_update_by_query'
        resp = self.request(endpoint=endpoint, refresh=refresh, body=body,
                            xdate=xdate, mode=mode)
        if resp.status_code != 200:
            logger = logging.getLogger(__name__)
            logger.info(f"Status {resp.status_code} {endpoint} date {xdate} " 
                        f"{refresh} {body}\n {resp.text}")
            return -1
        rj = resp.json()
        if rj.get('tagline'): # updateFields executed in fake mode 'f'
            return 1 # pretend everything is ok

        updated = rj.get('updated', 0)
        if any((updated < rj.get('total', 0), rj.get('timed_out'))):
            logger = logging.getLogger(__name__)
            logger.error(f"{name} {self.upd_bodies[name]} {rj}")
            return -1
        else:
            return updated

    def updateFieldsById(self, name: str, xid: str, values: dict=None,
                     xdate=None, seq_primary=None, refresh=None, mode=None):
        """
        Update fields for item with ident <xid>
        
        If <seq_primary> is specified as a tuple (item_seq, primary_term),
        updates only if the _item_seq and _primary_term of the item
        matches ones specified
        
        Returns the update response in form
        {'_index': ?, '_type': '_doc', '_id': ?, '_version': ?, 'result': 'updated',
         '_shards': {'total': ?, 'successful': ?, 'failed': ?},
         '_seq_no': ?, '_primary_term': ?}
        
        or None on failure
        """
        body = self.upd_bodies[name]
        if values:
            body['script']['params'] = values
        endpoint = '/'.join(('_doc', xid, "_update"))

        resp = self.request(endpoint=endpoint, seq_primary=seq_primary,
                            refresh=refresh, body=body, xdate=xdate, mode=mode)
        if resp.status_code != 200:
            logger = logging.getLogger(__name__)
            logger.info(f"Status {resp.status_code} {endpoint} date {xdate} " 
                        f"{seq_primary} {refresh} {body}\n {resp.text}")
            return None
        return resp.json()

    def _updFields(self, upd_fields=None, del_fields=None):
        upd_script =  [] if not upd_fields else \
            [f"ctx._source.{field}=params['{field}']" for field in upd_fields]
        del_script =  [] if not del_fields else   \
            [f"ctx._source.remove('{field}')" for field in del_fields]
        return ';'.join(upd_script+del_script)

# =============================================================================
#       Scroll API
# =============================================================================
    def scroll_set(self, body=None, mode=None):
        """
        Initializes a new scroll. Scroll is set no matter of mode value.
        
        Batch ends when scroll returns None
        """
        self.body = self._addFilter(body)
        if 'size' not in self.body:
            self.body['size'] = self.scroll_size
        self.endpoint_first = f'_search?scroll={self.keep}'
        self.endpoint_next = "_search/scroll"
        if mode:
            logger = logging.getLogger(__name__)
            logger.info(f"first {self.endpoint_first}, next {self.endpoint_next}"
                        f" body {body}\n")

        self.scroll_id = None
        self.buffer = None
        self.total_hits = None

    def scroll_total(self, mode=None):
        """
        Retrieves the total count of rows matching the request
        ES count method used as search does not return the exact count for
        large sets
        """
        return self.countIndex(self.body, mode=mode)

    def _scroll_next_batch(self, resp, mode=None):
        """
        Handles the next scroll batch from <resp> and sets the instance variables
        Mode not used in this call
        """
        if resp.status_code == 200:
            jresp = resp.json()
            # If no more data, buffer stays empty
            total_hits = jresp.get('hits',{}).get('total',{}).get('value',0)
            if  total_hits > 0:
                if not self.total_hits:
                    self.total_hits = total_hits
                self.buffer = jresp['hits'].get('hits',[])
                self.scroll_id = jresp['_scroll_id']
                self.body = {
                    'scroll': self.keep,
                    'scroll_id': self.scroll_id
                }
            return True
        else:
            logger = logging.getLogger(__name__)
            logger.error(f"{resp.status_code} {resp.text}")
            return False

    def scroll(self, mode=None):
        """
        Returns the next item from the scroll buffer (the item of ES hits list).
        
        If the process is not initialized yet executes the first scroll request
        If the buffer is empty, retrieves the next batch of items
        """
        assert hasattr(self, 'endpoint_first'), 'Scroll not initialized'
        if not self.buffer:
            # Buffer is empty, get new batch of data
            # If the buffer is not empty do nothing here but go and
            # return the next item from the batch
            if self.scroll_id:
                self._scroll_next_batch(
                    # Executes the request for each but the first batch
                    self.request(endpoint=self.endpoint_next,
                                 body=self.body, use_index_key=False, mode=mode),
                    mode=mode)
            else:
                # Executes the request for the first batch of items
                self._scroll_next_batch(
                    self.request(endpoint=self.endpoint_first, body=self.body,
                        mode=mode),
                    mode=mode)
            
        return None if not self.buffer else self.buffer.pop(0)

    def scroll_delete(self, mode=None):
        """
        Removes the scroll buffer.
        """
        body = {"scroll_id" : self.scroll_id}
        self.request(command='DELETE', endpoint='/_search/scroll',
                     body=body, mode=mode)
        
        
# =============================================================================
#       Bulk API
# =============================================================================
    def bulk_set(self, refresh=None, refresh_interval=None, bulk_max=None, mode=None):
        """
        Initializes a bulk index
        If included in the call bulk_max overrides one set on class initialization
        
        refresh is transfered to the request method. Possible values - None
        
        Sets the refresh interval if specified in <refresh_interval>
        
        Mode not used in this call
        """
        self.bulk_refresh = refresh
        if refresh_interval:
            self.setRefresh(period=refresh_interval)
        self.bulk_date_field = self.index.date_field
        if bulk_max:
            self.bulk_max = bulk_max
        self.__bulk_clear()

    def __bulk_clear(self):
        """
        Clears the bulk buffer, resets the bulk item counter and error flag
        """
        self.bulk = ''
        self.bulkcurr = 0
        self.bulkerror = False

    def bulk_index(self, item, action=None, xid=None, mode=None):
        """
        Adds the item data to the bulk. If bulk full flush it
        """
        assert self.bulkcurr is not None, "Bulk indexing not initialized! Execute bulk_set()"
        assert self.bulkcurr <= self.bulk_max, "bulk counter overflow"
        if self.bulkcurr == self.bulk_max:
            self.__bulk_flush(mode=mode)

        if not action:
            action = 'index' # Set index action if not specified
        # If span type is not n (date_field set) transfer the item date
        # as it is used to create the index name
        xdate = None if not self.bulk_date_field else item[self.bulk_date_field]
        bulk_action = self.__bulk_create_action(action=action, xid=xid, xdate=xdate)

        bulk_item = f"{bulk_action}\n{json.dumps(item)}\n"
        # input(bulk_item)
        self.bulk += bulk_item
        self.bulkcurr += 1

    def bulk_close(self, mode=None):
        """
        Flushes the last batch to the index and sets refresh interval to 1 second
        
        Returns True if no errors in flush and setRefresh
        """
        self.__bulk_flush(mode=mode)
        self.bulkcurr = None # indicates that bulk indexing is not initialized
        resp = self.setRefresh(period='1s')

        return all((not self.bulkerror, resp))

    def __bulk_flush(self, mode=None):
        """
        Flushes to the index and clears the bulk
        Sets the error flag if flush failed
        """
        if self.bulkcurr==0:
            return # nothing to flush
        resp = self._request_json(endpoint='_bulk', refresh=self.bulk_refresh,
                                  body=self.bulk, mode=mode)
        logger = logging.getLogger(__name__)
        if resp.status_code != 200:
            self.bulkerror = True
            logger.info(f"status {resp.status_code} error {resp.text}")
        elif resp.json().get('errors'):
            logger.info(f"error {resp.text}")
        self.__bulk_clear()

    def __bulk_create_action(self, action, xid=None, xdate=None):
        """
        Returns basic bulk action for bulk indexing
        Handles differences between ES versions prior to 7 (demands _type) and 7 (does not allow _type)
        """
        index_name = self.index.getIndexName(
            self.source, epoch=xdate)
        xaction = {"_index": index_name}
        if self.es_version < 7:
            xaction["_type"] = "_doc"
        if xid:
            xaction["_id"] = xid
        return json.dumps({action: xaction})
    
###############################################################################
    def termvectors(self, xid:str, xfield:str, mode=None):
        """
        Retrieves term vector data for the item <id>
        """
        endpoint = '/'.join(('_termvectors', xid))
        body = {"fields": [xfield],
                "offsets": False,
                "payloads": False,
                "positions": False,
                "term_statistics": True,
                "field_statistics": False,
                "filter": {
                    "max_num_terms": 10,
                    "min_term_freq": 1,
                    "min_doc_freq": 2
                  }
        }
        resp = self.request(body=body, endpoint=endpoint, mode=mode)        
        if resp.status_code != 200:
            logger = logging.getLogger(__name__)
            logger.info(f"status {resp.status_code} error {resp.text}")
            return None
        resp = resp.json()
        return resp['term_vectors'][xfield]['terms']
        
    def mlt(self, xids:list, mlt_conf:dict, mode=None)-> dict:
        """
        Retrieves more-like-this query for <xids> and configuration <mlt_conf>
        """
        pars = {"fields": mlt_conf['fields'], "like": [{"_id": x} for x in xids]}
        for field in ('min_term_freq', 'max_query_terms', 'min_doc_freq',
                      'max_doc_freq', 'min_word_length', 'max_word_length'):
            val = mlt_conf.get(field)
            if val is not None:
                pars[field] = val
        if mode:
            logger = logging.getLogger(__name__)
            logger.info(f"more-like-this {pars}")
        return {"more_like_this": pars}

# =============================================================================
    def __str__(self):
        return f"client={self.es_client}, source={self.source}"

    def __repr__(self):
        return "{self.__class__.__name__}({self.es_client},{self.es_version})"
