# -*- coding: utf-8 -*-
"""
Created on Wed Apr 14 10:56:39 2021

    Elasticsearch interface class. Provides easy handling of scroll and bulk
    requests as well as handling of time split indexes.

    External methods XElastic
        request
        usage
        get_indexes
        delete_indexes
    External methods XElasticIndex
        ========== Retrieve data
        get_data
        get_source_fields
        count_index
        query_index
        get_ids
        agg_index
        query_buckets
        query_cardinality
        ========== Handling spans
        index_name
        span_start
        span_end
        ========== Other
        create_term_filter
        mlt
        set_refresh
        save
    External methods XElasticUpdate
        set_upd_body
        update_fields
        update_fields_by_id
    External nethods XElasticScroll
        scroll_total
        scroll
        scroll_close
    External nethods XElasticBulk
        bulk_index
        bulk_close
        ==========

    The xelastic class uses index names of the format: prefix-stub-source-span
    Where
        prefix is shared by all indexes of the application
        stub identifies indexes of a particular type
        source identifies a particular data set
        span identifies a particular time period (span)

    prefix-stub is used in index templates thus these are indexes with identic
    settings and mappings

    esconf is the dictionary of the following form
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
                    date_field: <main date field of the index - the date field
                            to split the indexes on>, must be set for all span
                            types except n
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

@author: juris.rats
"""
# pylint: disable=logging-fstring-interpolation
import json
import copy
import logging
import urllib
import time
from datetime import datetime, timedelta
# Union, Set, List, Tuple, Collection, Any, Dict, Optional, NoReturn
from typing import Tuple, Any, Dict, Optional, Union, List

import requests
from requests.auth import HTTPBasicAuth

SPAN_ALL = 'all'        # span name for spantype == 'n'

class XElastic():
    """
    Elasticsearch interface class.
    """

    def __init__(self, esconf: dict, mode:Optional[str]=None):
        """
        Initializes the instance API for cluster level requests.
        
        Parameters:
            esconf: configuration dictionary for the Elasticsearch connection
            mode: may set mode for all requests for the current class instance

        If terms set queries and aggregations use this as additional filter
        (i.e. xelastic instance gets access to a part of the index)

        """
        self.mode = mode
        self.index_key = None

        self.source = esconf['source']

        # Retrieving specified connection and environment keys
        ckey = esconf['connection']['current']
        usr = esconf['connection'][ckey].get('usr')
        self.request_conf:Dict[str, str] = {
            'auth': None if not usr else HTTPBasicAuth(*usr),
            'timeout': esconf.get('timeout', 30), # Default to 30 secs,
            'verify': esconf['connection'][ckey].get('cert', False),
            'headers': esconf.get('headers',
                                      {"Content-Type": "application/json"})
            }
        self.es_client = esconf['connection'][ckey]['client']

        self.max_buckets = esconf.get('max_buckets', 99)

        # Retrieve the Elasticsearch version
        resp = self.request('GET', mode=mode).json()
        self.es_version = int(resp['version']['number'].split('.')[0])

        # self.set_source(self.source)

    def request(self, command:str='POST', endpoint:str='',
                seq_primary:Tuple[int, int]=None,
                refresh:Union[str, bool, None]=None, body:Dict[str, Any]=None,
                xdate:int=None, mode:Optional[str]=None) ->requests.Response:
        """
        Wrapper on _request_json. Converts dictionary <body> to json string
        
        Parameters:
            command: REST command
            endpoint: endpoint of the REST request
            seq_primary: tuple (if_seq_no, if_primary_term) for concurrency control
            refresh: 
                - not set or False: no refresh actions
                - wait_for: waits for the refresh to proceed
                - empty string or true (not recommended): immedially refresh the
                    relevant index shards
            body: body of the REST request
            xdate: date value used to determine the index (for time spanned indexes)
            use_index_key: If False index name is not appended to the endpoint
            mode:
                - None: (run) to run silently
                - f: (fake) to log parameters without running the request
                - v: (or any other value - verbose) to run and log data

        Returns:
            requests.Response object of the requests library

        Mode might be set by the methods parameter or by the instance variable
        If both set parameter has a precedence.

        NB!! Does not use self.terms
        """
        data = json.dumps(body) if body else None
        return self._request_json(command, endpoint, seq_primary, refresh, data,
                                  xdate, mode)

    def _request_json(self, command:str='POST', endpoint:str='',
            seq_primary:Tuple[int, int]=None, refresh:Union[str, bool, None]=None,
            data:str=None, xdate:int=None,
            mode:Optional[str]=None) ->requests.Response:
        """
        Wrapper to the requests method request.
        In most cases called from request method. Directly used e.g. for bulk
        indexing. 
        
        See descriptions of the request method for details. The only difference
        is the parameter 'data' which is a 'body' dictionary of the request 
        method converted to json string
        """
        mode = self._mode(mode)
        url = self.es_client
        if self.index_key:
            url += self.index_name(xdate) + '/'
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
                        f" url {url} body {data}")
        if mode == 'f':
            # execute dummy request
            return requests.request('GET', self.es_client, **self.request_conf)
        return requests.request(command, url, data = data, **self.request_conf)

    def usage(self, mode:Optional[str]=None):
        """
        Retrieves disk usage
        
        Parameters:
            mode: the mode parameter
            
        """
        endpoint = "_cat/allocation"
        resp = self.request(mode=self._mode(mode),
            command='GET', endpoint=endpoint)
        return int(resp.text.split()[5])

    def _mode(self, mode:str) ->str:
        """
        Parameters:
            mode: the mode parameter

        Returns:
            mode if not None, otherwise - self.mode
        """
        return mode if mode else self.mode

    # def set_source(self, source:str):
    #     """
    #     Changes the source
        
    #     Parameters:
    #         source: source key
    #     """
    #     self.source = source

    def _make_params(self, url:str, params:Dict[str, Any]) ->str:
        """
        Makes parameter string of form ?par1&par2 ... and appends it to the url
        
        Params:
            url: the url to append parameters to
            params: parameter dictionary (parameter name: value)
        
        Returns:
            url with parameter string appended
        """
        return '?'.join((url, urllib.parse.urlencode(params)))

    def get_indexes(self) ->list:
        """
        Return a list of existing index names for the index key
        """
        resp = self.request(command='GET', endpoint='_settings')
        if resp.status_code == 404:
            return []   # Mo indexes found, return empty list
        return list(resp.json().keys())

    def delete_indexes(self, indexes:list, mode:Optional[str]=None) ->bool:
        """
        Deletes indexes of the list <indexes>

        Parameters:
            indexes: a list of index names to delete indexes for
            mode: the mode parameter

        Returns:
            True if all indexes deleted succesfully
        """
        success = True
        for index in indexes:
            # set index name directly to handle indexes with time spans -
            # here indexes have to be deleted one by one
            resp = self.request(command="DELETE", endpoint=index,
                                mode=self._mode(mode))
            if not resp.json().get('acknowledged'):
                logger = logging.getLogger(__name__)
                logger.error(resp.text)
                success = False
        return success

# =============================================================================
#       Single index API
# =============================================================================
class XElasticIndex(XElastic):
    """
    Adding single index methods
    """

    def __init__(self, esconf: Dict[str, Any], index_key:Optional[str]=None,
                 terms:Optional[Dict[str, Any]]=None,
                 mode:Optional[str]=None):
        """
        Initializes the instance. See details in the parent method

        Parameters:
            esconf: configuration dictionary for the Elasticsearch connection
            index_key: the index key for the instance
            terms: terms dictionary of form {key1: value1, key2: value2, ...}
            body: query body to filter the items for scrolling
            mode: may set mode for all requests for the current class instance
        """
        super().__init__(esconf, mode)

        self.terms = terms
        self.index_key = index_key

        assert index_key in esconf['indexes'].keys(), \
                f"Wrong index key {index_key}"

        # Handle index configuration
        index_conf = esconf['indexes'][self.index_key]
        span_type = index_conf.get('span_type', 'n')
        self.span_conf = {
            'prefix': esconf['prefix'],
            'span_type': span_type,
            'date_field': index_conf.get('date_field'),
            'stub': index_conf['stub']
            }

        assert span_type in ('n','y','q','m','d'),\
            f"Wrong index span type {span_type}"
        assert any((span_type == 'n', self.span_conf['date_field'])), \
            f"Date field must be set for index of span type {span_type}"

# =============================================================================
#       Retrieve data
# =============================================================================
    def get_data(self, xid:str, mode:Optional[str]=None
                   ) ->Optional[Dict[str, Any]]:
        """
        Retrieve data for <xid> from the current index
        
        Parameters:
            xid: item id to retrieve the data from
            mode: mode parameter

        Returns:
            the full json (_source and metadata) or None if item with
                id <xid> not found
        """
        endpoint = '/'.join(('_doc', xid))
        resp = self.request(command='GET', endpoint=endpoint, mode=self._mode(mode))
        if resp.status_code != 200:
            logger = logging.getLogger(__name__)
            logger.error(resp.text, stack_info=True)
            return None
        return resp.json()

    def get_source_fields(self, xid:str, mode:Optional[str]=None
                 ) ->Optional[Dict[str, Any]]:
        """
        Retrieve data for <xid> from the current index
        
        Parameters:
            xid: item id to retrieve the data from
            mode: mode parameter
        
        Returns:
            the item data (_source) or None if item with id <xid> not found
        """
        resp = self.get_data(xid, self._mode(mode))
        return resp.get('_source') if resp else None

    def count_index(self, body:Dict[str, Any]=None, mode:Optional[str]=None
                    ) ->int:
        """
        Counts the items in index_key according to the criteria in body
        Adds self.terms filter if set

        Parameters:
            body: query body to filter items for counting
            mode: mode parameter

        Returns:
            item count for the given filter
        """
        resp = self.request(endpoint="_count", body=self._add_filter(body),
                            mode=self._mode(mode))
        if resp.status_code == 200:
            return resp.json()['count']

        logger = logging.getLogger(__name__)
        logger.error(resp.text, stack_info=True)
        return 0

    def query_index(self, body:Dict[str, Any]=None, mode:Optional[str]=None
                    ) -> Tuple[Dict[str, Any], int]:
        """
        Returns adictionary of requested rows and total count of matching rows
        
        Parameters:
            body: query body
            mode: mode parameter

        Returns:
            a list of query results and a number of matching items

        If no results - returns empty list and 0
        When error returns empty list and the error type (string)
        Adds self.terms filter if set
        """
        resp = self.request(endpoint="_search", body=self._add_filter(body),
                            mode=self._mode(mode))
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

    def get_ids(self, body:Dict[str, Any]=None, mode:Optional[str]=None
                    ) -> List[str]:
        """
        Returns list of ids of documents mathcing the query <body>
        
        Parameters:
            body: query body
            mode: mode parameter

        Returns:
            a list of ids

        If no results - returns empty list
        """
        # Ensure _source fields are not included in the returned results
        body['_source'] = False
        hits, _ = self.query_index(body, mode=self._mode(mode))
        return [hit['_id'] for  hit in hits]

    def agg_index(self, body:Dict[str, Any], mode:Optional[str]=None
                  ) -> Optional[list]:
        """
        Executes the aggregate request specfied by the body parameter.
        
        Parameters:
            body: a body of the aggregate request
            mode: the mode parameter

        Returns:
            the aggregations dictionary returned by Elasticsearch
                aggregation request

        Adds self.terms filter if set
        """
        resp = self.request(endpoint="_search", body=self._add_filter(body),
                            mode=self._mode(mode))
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

    def query_buckets(self, field:str, query:Dict[str, Any]=None,
                     max_buckets:int=None, quiet:bool=False,
                     mode:Optional[str]=None) ->Tuple[Dict[str, int], int]:
        """
        Retrieves the buckets data on <field>.

        Parameters:
            field: the field name to get buckets for
            query: a query dictionary used to filter the items to aggregate
            max_buckets: max number if buckets to retrieve; set to
                self.max_buckets if not specified in the parameter
            quiet: if True log the case when there are more than max_buckets
                buckets available
            mode: the mode parameter

        Returns:
            a dictionary of form key: doc count, and the number of not
                aggregated documents
        """
        mbuckets = max_buckets if max_buckets else self.max_buckets
        body = {"size": 0,
                "aggs": {"agg": {"terms": {"field": field, "size": mbuckets}}}}
        if query:
            body['query'] = query
        aggs = self.agg_index(body, self._mode(mode))
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

    def query_cardinality(self, field: str, mode:Optional[str]=None) -> int:
        """
        Retrieve the number of unique values of <field> (cardinality)

        Parameters:
            field: the field name to get cardinality for
            mode: the mode parameter

        Returns:
            The cardinality of the specified field

        Adds self.terms filter if set
        """
        body = {"size": 0,
          "aggs": {
            "agg": {
              "cardinality": {
                "field": field
        }}}}
        resp = self.request(endpoint='_search', body=self._add_filter(body),
                            mode=self._mode(mode))
        assert resp.status_code == 200, resp.text

        return resp.json()["aggregations"]["agg"]["value"]

    def _add_filter(self, body:Dict[str, Any]=None, mode:Optional[str]=None
                   ) -> Dict[str, Any]:
        """
        Adds the terms filters for self.terms to the body filters.

        Parameters:
            body: the main filter
            mode: the mode parameter

        Returns:
            the merged filter

        Uses a copy of the body parameter to avoid changing the parameter value

        If self.terms not set just returns body
        """
        assert any((body is None, isinstance(body, dict))), 'body must be a dict'
        if not self.terms:
            return {} if body is None else body

        xbody = {} if body is None else copy.deepcopy(body)
        xfilter = self.create_term_filter(self.terms)
        query = xbody.get('query')
        if not query:
            # If body has no query set query to the terms filter
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
        if self._mode(mode):
            logger = logging.getLogger(__name__)
            logger.info("_add_filter %s", xbody)

        return xbody

# =============================================================================
#       Handling spans
# =============================================================================
    def index_name(self, epoch:int=None) ->Optional[str]:
        """
        Get index name for the stub, source and epoch.

        Parameters:
            epoch: time as epoch or None to specify all time spans

        Returns:
            Index name for current time span and index_key

        If span_type == 'n' SPAN_ALL is used as span in the index name
        Otherwise
            if epoch set span is calculated from the epoch
            else * is used for span (all spans addressed)
        """
        if self.span_conf['span_type'] == 'n':
            span = SPAN_ALL
        elif not epoch:
            span = '*'
        else:
            local = time.localtime(epoch)
            try:
                span = {
                    'y': time.strftime("%Y", local),
                    'q': '-'.join((time.strftime("%Y", local),
                               str(int(time.strftime("%m", local)) // 3 + 1))),
                    'm': '-'.join((time.strftime("%Y", local),
                                   time.strftime("%m", local))),
                    'd': '-'.join((time.strftime("%Y", local),
                        time.strftime("%m", local), time.strftime("%d", local)))
                }[self.span_conf['span_type']]
            except KeyError:
                return None
        return '-'.join((self.span_conf['prefix'], self.span_conf['stub'],
                         self.source, span))

    def span_start(self, span: str) -> Optional[int]:
        """
        Parameters:
            span: The span part of the index name, forma depends on span_type
                'yyyy' (y), 'yyyy-mm' (m or q), 'yyyy-mm-dd' (d)

        Returns:
            The start time of the span (epoch)
        """
        span_type = self.span_conf['span_type']
        if span_type=='y':
            return int(datetime(int(span),1,1).timestamp())
        if span_type=='q':
            year, quarter = span.split('-')
            return int(datetime(int(year), int(quarter) * 3 - 2, 1).timestamp())
        if span_type=='m':
            year, month = span.split('-')
            return int(datetime(int(year), int(month), 1).timestamp())
        if span_type=='d':
            year, month, day = span.split('-')
            return int(datetime(int(year), int(month), int(day)).timestamp())

        return None

    def span_end(self, span: str) -> Optional[int]:
        """
        Parameters:
            span: The span part of the index name, forma depends on span_type
                'yyyy' (y), 'yyyy-mm' (m or q), 'yyyy-mm-dd' (d)

        Returns:
            The end time of the span (epoch)
        """
        span_type = self.span_conf['span_type']
        if span_type=='y':
            return int(datetime(int(span)+1, 1, 1).timestamp())
        if span_type=='q':
            year, quarter = span.split('-')
            year, month = self._next_month(int(year), int(quarter) * 3)
            return int(datetime(year, month, 1).timestamp())
        if span_type=='m':
            year, month = span.split('-')
            year, month = self._next_month(int(year), int(month))
            return int(datetime(year, month, 1).timestamp())
        if span_type=='d':
            year, month, day = span.split('-')
            return int((datetime(int(year), int(month), int(day)) +
                       timedelta(days=1)).timestamp())

        return None

    def _next_month(self, xyear: int, xmonth: int) -> Tuple[int, int]:
        """
        Parameters:
            xyear: year (number)
            xmonth: month (number)

        Returns:
            year and month number for the next month relative to xyear
                and xmonth
        """
        return (xyear, xmonth + 1) if xmonth < 12 else (xyear + 1, 1)

# =============================================================================
#       Other
# =============================================================================
    def create_term_filter(self, terms:Dict[str, Any]) ->list:
        """
        Creates terms filter ([{"term": {<field>: <value>}}, ...]) from the
        <terms> dictionary

        Parameters:
            terms: the dictionary of field names and values

        Returns:
            The list of term filters
        """
        return [{"term": {key, val}} for key, val in terms]

    def mlt(self, xids:list, mlt_conf:Dict[str, Any], mode:Optional[str]=None
            )-> Dict[str, Any]:
        """
        Retrieves more-like-this query for <xids> and configuration <mlt_conf>

        Parameters:
            mlt_conf: configuration dictionary for Elasticsearch mlt query
            mode: the mode parameter

        Returns:
            more_like_this dictionary ready for usr in mlt query           
        """
        pars = {"fields": mlt_conf['fields'], "like": [{"_id": x} for x in xids]}
        for field in ('min_term_freq', 'max_query_terms', 'min_doc_freq',
                      'max_doc_freq', 'min_word_length', 'max_word_length'):
            val = mlt_conf.get(field)
            if val is not None:
                pars[field] = val
        if self._mode(mode):
            logger = logging.getLogger(__name__)
            logger.info(f"more-like-this {pars}")
        return {"more_like_this": pars}

    def set_refresh(self, period:str='1s', mode:Optional[str]=None) -> bool:
        """
        Sets refresh interval for the current index of key index_key

        Parameters:
            period: refresh period to set
            mode: the mode parameter

        Returns:
            True if the period is set, False otherwise

        Period has form 'xxxs' where xxx is number of seconds
        """
        body = {"index": {"refresh_interval": period}}
        resp = self.request(command='PUT', endpoint='_settings', body=body,
                            mode=self._mode(mode))
        result = True
        if resp.status_code != 200:
            result = False
            logger = logging.getLogger(__name__)
            logger.info(f"Status {resp.status_code} _settings "
                        f"{body}\n {resp.text}")
        return result

    def save(self, body:dict, xid:str=None, seq_primary:Tuple[int, int]=None,
             xdate:int=None, refresh:Union[str, bool, None]=None, mode:str=None
             ) ->str:
        """
        Index an item
        Adds to the data body self.terms to save the data of the terms fields
        
        Parameters:
            body: body of the REST request
            xid: ID of the item to save data to
            seq_primary: tuple (if_seq_no, if_primary_term) for cuncurrency control
            xdate: date value used to determine the index (for time spanned indexes)
            refresh: see description fro request method
            mode: see description fro request method
        
        Returns:
            id of the created item or None on failure
        """
        if self.terms:
            for key, val in self.terms.items():
                body[key] = val
        endpoint = '_doc/'
        if xid:
            endpoint += xid

        resp = self.request(endpoint=endpoint, seq_primary=seq_primary,
                            refresh=refresh, body=body,
                            xdate=xdate,  mode=self._mode(mode))
        if resp.status_code != 201: # resource created
            logger = logging.getLogger(__name__)
            logger.error(resp.text, stack_info=True)
            return None
        return resp.json()['_id']



###############################################################################

    def __str__(self):
        return f"client={self.es_client}, source={self.source}"

    def __repr__(self):
        return "{self.__class__.__name__}({self.es_client},{self.es_version})"


# =============================================================================
#       Update API
# =============================================================================
class XElasticUpdate(XElasticIndex):
    """
    Adding scroll methods to the XElastic
    """

    def __init__(self, esconf: Dict[str, Any], index_key:Optional[str]=None,
                 terms:Optional[Dict[str, Any]]=None,
                 mode:Optional[str]=None):
        """
        Initializes the instance. See details in the parent method

        Parameters:
            esconf: configuration dictionary for the Elasticsearch connection
            index_key: the index key for the instance
            terms: terms dictionary of form {key1: value1, key2: value2, ...}
            body: query body to filter the items for scrolling
            mode: may set mode for all requests for the current class instance
        """
        super().__init__(esconf, index_key, terms, mode)

        self.upd_bodies: Dict[str, Dict[str, Any]] = {} # placeholder for update scripts

    def set_upd_body(self, name: str, upd_fields: list = None,
                     del_fields: list = None):
        """
        Create and save in upd_bodies the update dictionary. Uses _upd_fields to
        create script source.
        
        Parameters.
            name: name of the update script
            upd_fields: fields to update
            del_fields: fields to remove
        """
        self.upd_bodies[name] = {"script": {
                "source": self._upd_fields(upd_fields, del_fields),
                "lang": "painless"}}

    def update_fields(self, name: str, xfilter: dict, values: dict=None,
                     xdate=None, refresh:Union[str, bool, None]=None,
                     mode:Optional[str]=None) ->int:
        """
        Update / delete fields for items filtered by xfilter (update by query)
        If update body <name> has update fields, <values> must be specified

        Parameters.
            name: name of the update body (created by set_upd_body)
            xfilter: query to select items for update
            values: a dictionary of field names and values, names must match
                what is set in set_upd_body
            xdate: value of the main date field of the item to update; used 
                    to identify the index the item is saved in
            refresh:
                - not set or False - no refresh actions
                - wait for - waits for the refresh to proceed
                - empty string or true (not recommended) - immedially refreh the
                      relevant index shards
            mode: the mode parameter

        Returns:
            number of updated items if updates successful, -1 otherwise

        Logs errors on failure
        {"took": ?, "timed_out": false, "total": ?, "updated": ?, ...}
        """
        body = self.upd_bodies[name]
        body['query'] = xfilter
        if values:
            body['script']['params'] = values
        endpoint = '_update_by_query'
        resp = self.request(endpoint=endpoint, refresh=refresh, body=body,
                            xdate=xdate, mode=self._mode(mode))
        if resp.status_code != 200:
            logger = logging.getLogger(__name__)
            logger.info(f"Status {resp.status_code} {endpoint} date {xdate} "
                        f"{refresh} {body}\n {resp.text}")
            return -1
        resp_json = resp.json()
        if resp_json.get('tagline'): # update_fields executed in fake mode 'f'
            return 1 # pretend everything is ok

        updated = resp_json.get('updated', 0)
        if any((updated < resp_json.get('total', 0), resp_json.get('timed_out'))):
            logger = logging.getLogger(__name__)
            logger.error(f"{name} {self.upd_bodies[name]} {resp_json}")
            return -1

        return updated

    def update_fields_by_id(self, name: str, xid: str, values:Dict[str, Any]=None,
            xdate:int=None, seq_primary:tuple=None, refresh:Optional[str]=None,
            mode:Optional[str]=None) ->Optional[Dict[str, Any]]:
        """
        Update fields for item with ident <xid>
        
        Parameters:
            name: name of the update body (created by set_upd_body)
            xid:id of the item to update
            values: a dictionary of field names and values, names must match
                what is set in set_upd_body
            xdate: value of the main date field of the item to update; used 
                    to identify the index the item is saved in
            refresh:
                - not set or False - no refresh actions
                - wait for - waits for the refresh to proceed
                - empty string or true (not recommended) - immedially refreh the
                      relevant index shards
            mode: the mode parameter

        Returns:
            the update response

        update response has form
            {'_index': ?, '_type': '_doc', '_id': ?, '_version': ?,
            'result': 'updated', '_shards': {'total': ?, 'successful': ?, 'failed': ?},
            '_seq_no': ?, '_primary_term': ?}
            or None on failure        
        
        If <seq_primary> is specified as a tuple (item_seq, primary_term),
        updates only if the _item_seq and _primary_term of the item
        matches ones specified
        """
        assert any((self.span_conf['span_type']=='n', xdate)), \
            "xdate must be specified for all span types except 'n'"
        body = self.upd_bodies[name]
        if values:
            body['script']['params'] = values
        endpoint = '/'.join(('_update', xid))

        resp = self.request(endpoint=endpoint, seq_primary=seq_primary,
                            refresh=refresh, body=body, xdate=xdate,
                            mode=self._mode(mode))
        if resp.status_code != 200:
            logger = logging.getLogger(__name__)
            logger.error(f"Status {resp.status_code} {endpoint} date {xdate} "
                        f"{seq_primary} {refresh} {body}\n {resp.text}")
            return None
        return resp.json()

    def _upd_fields(self, upd_fields:list=None, del_fields:list=None) -> str:
        """
        Creates and returns the source of the update script
        
        Parameters:
          upd_fields - list of the names of fields to update
          del_fields - list of the names of fields to delete

        Returns:
            update script
        """
        upd_script =  [] if not upd_fields else \
            [f"ctx._source.{field}=params['{field}']" for field in upd_fields]
        del_script =  [] if not del_fields else   \
            [f"ctx._source.remove('{field}')" for field in del_fields]
        return ';'.join(upd_script+del_script)


# =============================================================================
#       Scroll API
# =============================================================================
class XElasticScroll(XElasticIndex):
    """
    Adding scroll methods to the XElastic
    """

    def __init__(self, esconf: Dict[str, Any], index_key:Optional[str]=None,
                 terms:Optional[Dict[str, Any]]=None,
                 body:Optional[Dict[str, Any]]=None,
                 mode:Optional[str]=None):
        """
        Initializes the instance. See details in the parent method

        Parameters:
            esconf: configuration dictionary for the Elasticsearch connection
            index_key: the index key for the instance
            terms: terms dictionary of form {key1: value1, key2: value2, ...}
            body: query body to filter the items for scrolling
            mode: may set mode for all requests for the current class instance
        """
        super().__init__(esconf, index_key, terms, mode)

        scroll_body = self._add_filter(body)
        if 'size' not in scroll_body:
            scroll_body['size'] = esconf.get('scroll_size', 100)

        # Configuration for the scroll API
        keep = esconf.get('keep', '10s')
        self.scroll_conf:Dict[str, Any] = {
            'id': None,  # Identifies the first scroll batch
            'body': scroll_body,
            'keep': keep,
            'endpoint_first': f"_search?scroll={keep}",
            'endpoint_next': "_search/scroll"}

    def scroll_total(self, mode:Optional[str]=None) ->int:
        """
        Retrieves the total count of rows matching the scroll request

        Parameters:
            mode: the mode parameter

        Returns:
            The total number of items matching the scroll request
        """
        return self.count_index(self.scroll_conf['body'], mode=self._mode(mode))

    def _scroll_next_batch(self, resp:requests.Response) ->bool:
        """
        Handles the next scroll batch from <resp> and sets the instance variables

        Parameters:
            resp: the response of the request
            mode: the mode parameter

        Returns:
            True on success, False otherwise
        """
        if resp.status_code == 200:
            jresp = resp.json()
            # If no more data, buffer stays empty
            total_hits = jresp.get('hits',{}).get('total',{}).get('value',0)
            if  total_hits > 0:
                if not self.scroll_conf.get('total_hits'):
                    self.scroll_conf['total_hits'] = total_hits
                self.scroll_conf['buffer'] = jresp['hits'].get('hits',[])
                self.scroll_conf['id'] = jresp['_scroll_id']
                self.scroll_conf['body'] = {
                    'scroll': self.scroll_conf['keep'],
                    'scroll_id': self.scroll_conf['id']
                }
            return True

        logger = logging.getLogger(__name__)
        logger.error(f"{resp.status_code} {resp.text}")
        return False

    def scroll(self, mode:Optional[str]=None) ->Optional[Dict[str, Any]]:
        """
        Parameters:
            mode: the mode parameter

        Returns:
            the next item from the scroll buffer (the item of ES hits list).
        
        If the process is not started yet executes the first scroll request
        If the buffer is empty, retrieves the next batch of items
        """
        mode = self._mode(mode)
        if not self.scroll_conf.get('buffer'):
            # Buffer is empty, get new batch of data
            # If the buffer is not empty do nothing here but go and
            # return the next item from the batch
            if self.scroll_conf['id'] is None:
                # Executes the request for the first batch of items
                self._scroll_next_batch(
                    self.request(endpoint=self.scroll_conf['endpoint_first'],
                                 body=self.scroll_conf['body'], mode=mode))
            else:
                self._scroll_next_batch(
                    # Executes the request for each but the first batch
                    self.request(endpoint=self.scroll_conf['endpoint_next'],
                    body=self.scroll_conf['body'], mode=mode))

        return None if not self.scroll_conf['buffer'] else \
            self.scroll_conf['buffer'].pop(0)

    def scroll_close(self, mode:Optional[str]=None) ->None:
        """
        Removes the scroll buffer.

        Parameters:
            mode: the mode parameter
        """
        body = {"scroll_id" : self.scroll_conf['id']}
        del self.scroll_conf['body']
        self.request(command='DELETE', endpoint='/_search/scroll',
                     body=body, mode=self._mode(mode))


# =============================================================================
#       Bulk API
# =============================================================================
class XElasticBulk(XElasticIndex):
    """
    Adding bulk indexing methods to the XElastic
    """
    def __init__(self, esconf: Dict[str, Any], index_key:str=None,
                 terms:Optional[Dict[str, Any]]=None,
                 refresh:Union[str, bool, None]=None, refresh_interval:str=None,
                 bulk_max:int=None, mode:Optional[str]=None):
        """
        Initializes the instance. See details in the parent method

        Parameters:
            esconf: configuration dictionary for the Elasticsearch connection
            index_key: the index key for the instance
            terms: terms dictionary of form {key1: value1, key2: value2, ...}
            refresh: refresh type for the bulk requests
            refresh_interval: refresh interval to set for the bulk requests
            bulk_max: max items in the bulk buffer, overrides the one set in
                esconf
            mode: may set mode for all requests for the current class instance
        """
        super().__init__(esconf, index_key, terms, mode)

        self.mode = self._mode(mode) # Setmode for use in calls of the current bulk

        if refresh_interval:
            self.set_refresh(period=refresh_interval)

        # Configuration for the bulk API
        xmax = bulk_max if bulk_max else esconf.get('index_bulk', 1000)

        self.bulk_conf:Dict[str, Any] = {
            'max': xmax,
            'main_mode': self.mode,  # saved to restore mode when close bulk
            'refresh': refresh
            }
        self._bulk_clear()

    def _bulk_clear(self):
        """
        Clears the bulk buffer, resets the bulk item counter and error flag
        """
        self.bulk_conf['buffer'] = ''
        self.bulk_conf['curr'] = 0
        self.bulk_conf['error'] = False

    def bulk_index(self, item:Dict[str, Any], action:str=None, xid:str=None,
                   mode:Optional[str]=None) ->None:
        """
        Adds the item data to the bulk. If bulk full flush it
        
        Parameters:
            item: the dictionary of data to add to the bulk idexing buffer
            action: indexing action [index or update], defaults to index
            xid: id of the item to index, if None id is generated by ES
            mode: the mode parameter
        """
        assert self.bulk_conf['curr'] <= self.bulk_conf['max'], \
            "bulk counter overflow"

        if self.bulk_conf['curr'] == self.bulk_conf['max']:
            self._bulk_flush(mode=self._mode(mode))

        if not action:
            action = 'index' # Set index action if not specified
        # If span type is not n (date_field set) transfer the item date
        # as it is used to create the index name
        date_field = self.span_conf.get('date_field')
        xdate = None if not date_field else item[date_field]
        bulk_action = self._bulk_create_action(action=action, xid=xid, xdate=xdate)

        bulk_item = f"{bulk_action}\n{json.dumps(item)}\n"
        # input(bulk_item)
        self.bulk_conf['buffer'] += bulk_item
        self.bulk_conf['curr'] += 1

    def bulk_close(self, mode:Optional[str]=None) ->bool:
        """
        Flushes the last batch to the index and sets refresh interval to 1 second
        
        Parameters:
            mode: the mode parameter

        Returns:
            True if no errors in flush and set_refresh
        """
        self._bulk_flush(mode=self._mode(mode), refresh=self.bulk_conf['refresh'])
        # Resets the main mode of the class instance
        self.mode = self.bulk_conf['main_mode']
        # indicates that bulk indexing is not initialized
        self.bulk_conf['curr'] = None
        resp = self.set_refresh(period='1s')

        return all((not self.bulk_conf['error'], resp))

    def _bulk_flush(self, refresh:Union[str, bool, None]=None,
                    mode:Optional[str]=None):
        """
        Flushes to the index and clears the bulk
        Sets the error flag if flush failed

        Parameters:
            refresh: index refresh type (None, wait_for or True)
            mode: the mode parameter
        """
        if self.bulk_conf['curr'] == 0:
            return # nothing to flush
        resp = self._request_json(endpoint='_bulk', refresh=refresh,
                    data=self.bulk_conf['buffer'], mode=self._mode(mode))
        logger = logging.getLogger(__name__)
        if resp.status_code != 200:
            self.bulk_conf['error'] = True
            logger.info(f"status {resp.status_code} error {resp.text}")
        elif resp.json().get('errors'):
            logger.info(f"error {resp.text}")
        self._bulk_clear()

    def _bulk_create_action(self, action:str, xid:str=None, xdate:int=None):
        """
        Parameters:
            action: indexing action (index or update)
            xid: id of the item to index, if None id is generated by ES
            mode: the mode parameter

        Returns:
            A basic bulk action for bulk indexing

        Handles differences between ES versions prior to 7 (demands _type) and
        7 (does not allow _type)
        """
        index_name = self.index_name(epoch=xdate)
        xaction = {"_index": index_name}
        if self.es_version < 7:
            xaction["_type"] = "_doc"
        if xid:
            xaction["_id"] = xid
        return json.dumps({action: xaction})
