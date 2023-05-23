# -*- coding: utf-8 -*-
"""
Created on Thu Apr 27 13:04:07 2023

@author: juris.rats
"""
import logging
# Union, Set, List, Tuple, Collection, Any, Dict, Optional, NoReturn
from typing import Dict, Any

import sys, os
sys.path.append("..")
from src.xelastic import XElastic

def set_templates(esconf:Dict[str, Any], template_data: Dict[str, Any]):
    """
    Creates templates for index_keystes exist for <indices> and create if not
    
    Any elastic class instance <es> can be used as index key is not used
    """
    es = XElastic(esconf)
    # Retrieve configuration data for all indices
    for xkey, xval in es.indexes.items():
        template_conf = template_data[xkey]
        template = es.make_template(xkey, **template_conf,
                                    refresh_interval=xval.get('ri'))

        # Create the index template
        try:
            es.set_template(xkey, template)
        except ConnectionError as err:
            print(f"Connection error: {err}")
            sys.exit(1)
        except:
            raise

        # Print template
        print(f"==={xkey}===\n{es.get_template(xkey)}")

###############################################################################
logging.basicConfig(handlers=[logging.StreamHandler()], level=logging.INFO,
    format= "%(asctime)s [%(filename)s:%(lineno)s - %(funcName)s() ] %(message)s")

conf = {
    'connection': {'client': os.environ.get('ELASTICSEARCH_URL')},
    'prefix': 'ta',
    'source': 'src',
    'timeout': 10,
    'indexes': {
        'customers': {'stub': 'cst', 'span_type': 'm',
                      'date_field': 'created', 'refresh': '60s'}}
   }

tconf = {
    'customers': {
        'index': {"number_of_shards": "1", "number_of_replicas": "0"},
        'description':  "sample customer data",
        'properties': {
            "name": {"type": "keyword"},
            "email": {"type": "keyword"},
            "phone": {"type": "keyword"},
            "created": {"type": "date", "format": "epoch_second"}
        }
    }
}

set_templates(conf, tconf)

