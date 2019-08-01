import pandas as pd
from pandas.io.json import json_normalize
from elasticsearch import Elasticsearch
import yaml
conf_vars = yaml.load(open('tests_conf.yaml'))
ELASTICSEARCH_IP=conf_vars.get('elasticsearch_ip', '9.148.244.26')
ELASTICSEARCH_PORT=conf_vars.get('elasticsearch_port', '30777')
es = Elasticsearch(
       	[ELASTICSEARCH_IP],scheme='http',port=ELASTICSEARCH_PORT,
)
for index in es.indices.get('*flow*'):
        print index
 	es.indices.delete(index=index)


