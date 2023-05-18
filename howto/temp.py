# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 11:33:18 2023

@author: juris.rats
"""
import logging, os, sys

sys.path.append("..")
from src.xelastic import XElastic, ConnectionError

def configure_logger(root:str, conf:dict, script_name:str=None):
    """
    Configures logging as specified in the parameters. Uses force=True
    to remove any previous configurations (possibly ce=reated by other scripts)
    as python basic logging does not create handlers if any handlers already 
    created
    """
    # log_name = script_name + '_log.txt'
    handler = logging.FileHandler(os.path.join(root, 'logs', script_name + '_log.txt')) \
        if script_name else logging.StreamHandler()                                  
    logging.basicConfig(handlers=[handler],
                        level=conf.get('level', 20),
                        format=conf.get('format'),
                        datefmt=conf.get('datefmt'),
                        force=True)

conf = {
    'connection': {
        'current': 'docker',
        'local': {'client': 'http://localhost:9200/'},
        'docker': {'client': 'http://elasticsearch:9200/'}},
    'prefix': 'ta',
    'source': 'src',
    'timeout': 10,
    'indexes': {
        'customers': {'stub': 'cst', 'span_type': 'm',
                      'date_field': 'created', 'refresh': '60s'}},
    'logger': {
        'level': 20,  # NOTSET=0, DEBUG=10, INFO=20, WARN=30, ERROR=40, CRITICAL=50
        'format': "%(asctime)s [%(filename)s:%(lineno)s - %(funcName)s() ] %(message)s",
        'datefmt': "%Y-%m-%d %H:%M:%S"
    }
}

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
configure_logger(ROOT, conf['logger'])

# logger = logging.getLogger(__name__)
# logger.info('Te es esmu')

index = {"number_of_shards": "1", "number_of_replicas": "0"}
description = "sample customer data"
properties = {
    "name": {"type": "keyword"},
    "email": {"type": "keyword"},
    "phone": {"type": "keyword"},
    "created": {"type": "date", "format": "epoch_second"}
}

xkey = 'customers'
try:
    es = XElastic(conf)
except ConnectionError as err:
    print('Not available')
    sys.exit(1)
except:
    print('Error')
    print(sys.exc_info())
    sys.exit(2)
template = es.make_template(xkey, index=index, properties=properties,
                          description=description)
try:
    es.set_template(xkey, template)
except:
    print('Template creation failed')
    sys.exit(3)

# Print template
print(f"==={xkey}===\n{es.get_template(xkey)}")
