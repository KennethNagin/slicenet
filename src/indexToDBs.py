import pandas as pd
from pandas.io.json import json_normalize
from elasticsearch import Elasticsearch
from influxdb import DataFrameClient, InfluxDBClient
import argparse
from skydive.rest.client import RESTClient
import yaml
conf_vars = yaml.load(open('tests_conf.yaml'))
ELASTICSEARCH_IP=conf_vars.get('elasticsearch_ip', '9.148.244.26')
ELASTICSEARCH_PORT=conf_vars.get('elasticsearch_port', '30777')
def getDBIndexs(file_name):
	df = pd.read_csv(file_name)
	print("index begin",df.begin.min())
	print("index end",df.end.max())
	qosBefore = df.qosBefore.max()
	qosAfter = df.qosAfter.max()
	qoeBefore = df.qoeBefore.min()
	qoeAfter = df.qoeAfter.max()
	print("index shape",df.shape)
	return(qosBefore,qosAfter,qoeBefore,qoeAfter)
def createSkydiveEsCsv(csv,before,after):
	print('before',before)
	print('after',after)
	es = Elasticsearch(
        	[ELASTICSEARCH_IP],scheme='http',port=ELASTICSEARCH_PORT,
	)		
 	# Initialize the scroll
  	page = es.search(
  		doc_type = 'flow',
  		scroll = '2m',  		
  		size = 1000,
  		body = {
    			"query" : {
        			"match_all" : {}
    			}
		})
	sid = page['_scroll_id']
  	scroll_size = page['hits']['total']
	df = json_normalize(page['hits']['hits'])
	# Start scrolling
	print("shape",df.shape)
	frames = [df]	
  	while (scroll_size > 0):
    		print "Scrolling..."
    		page = es.scroll(scroll_id = sid, scroll = '2m')
    		# Update the scroll ID
    		sid = page['_scroll_id']
    		# Get the number of results that we returned in the last scroll
    		scroll_size = len(page['hits']['hits'])
    		print "scroll size: " + str(scroll_size)
		df = json_normalize(page['hits']['hits'])
		print("shape",df.shape)
		frames.append(df)
    		# Do something with the obtained page
	print("#frames",len(frames))
	df = pd.concat(frames,sort=False)
	#df = df[(df['_source.Metric.Last'] > before) & (df['_source.Metric.Last'] <= after)]
	#df = df[df['_source.Metric.Last'].between(before,after,inclusive=True)]	
	print("SkydiveEs shape",df.shape)
	df.to_csv(csv)
def createJmeterInfluxDBCsv(csv,before,after):
	print('before',before)
	print('after',after)
	client=InfluxDBClient(host=u'9.148.244.43', port=31701, database=u'jmeter',  proxies=None)
	query="select * from jmeter"
	points = client.query(query, chunked=True, chunk_size=10000,epoch='ms').get_points()
	df = pd.DataFrame(points)
	#df = df[(df['time'] > before) & (df['time'] <= after)]
	print("jmeter shape",df.shape)
	df.to_csv(csv)
	
if __name__ == "__main__":
        parser = argparse.ArgumentParser(description='create skydiveFlows.csv and jmeter.csv from workload_stress_index')
        parser.add_argument('-s, --skydiveFlowsCsv', action="store", dest="skydiveFlowsCsv", default='skydiveFlows.csv', help='skydiveFlows.csv')
        parser.add_argument('-j, --jmeterCsv', action="store", dest="jmeterCsv", default='jmeter.csv', help='jmeter.csv')
        parser.add_argument('-i, --indexCsv', action="store", dest="indexCsv", default='workload_stress_index.csv',  help='index cvs file to read ')

        parms = parser.parse_args()
        qosBefore, qosAfter, qoeBefore, qoeAfter = getDBIndexs(parms.indexCsv)
        createSkydiveEsCsv(parms.skydiveFlowsCsv,qosBefore,qosAfter)
	createJmeterInfluxDBCsv(parms.jmeterCsv,qoeBefore,qoeAfter)

