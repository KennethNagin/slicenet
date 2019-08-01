import os
import subprocess
from time import sleep
import time 
import threading
from threading import Thread
import paramiko
import logging
import yaml
import mmap
import re
import json
import csv
from elasticsearch import Elasticsearch
from influxdb import DataFrameClient, InfluxDBClient
import pandas as pd

conf_vars = yaml.load(open('tests_conf.yaml'))
# Global variables
LOG_FILE_NAME = conf_vars.get('logFileName', "iperf_tests.log")
WORKLOAD_LABEL=conf_vars.get('workload_label', 'wp2.jmx')
ITERATIONS=conf_vars.get('iterations', 1)
WORKLOAD=conf_vars.get('workload', 'my_workload.sh')
STRESSERS=conf_vars.get('stressers',[])
ELASTICSEARCH_IP=conf_vars.get('elasticsearch_ip', '9.148.244.26')
ELASTICSEARCH_PORT=conf_vars.get('elasticsearch_port', '30777')
WORKLOAD_STRESS_INDEX=conf_vars.get('workload_stress_index', 'workload_stress_index.csv')


# Set up logging to file
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%d/%m/%y %H:%M:%S',
                    filename=LOG_FILE_NAME)

# Define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.INFO)

# Set a simple format for console use
formatter = logging.Formatter('%(levelname)-8s: %(message)s')
console.setFormatter(formatter)

# Add the handler to the root logger
logging.getLogger('').addHandler(console)


class threadStress(threading.Thread):
   def __init__(self, stresser,host,parms):
      threading.Thread.__init__(self)
      self.stresser = stresser
      self.host = host
      self.parms = parms
   def run(self):
      logging.info("thread stress starttime %d",int(time.time()*1000.0))
      #subprocess.call("ssh {} {} {}".format(self.host,self.stresser,self.parms), shell=True)
      ps = subprocess.Popen("ssh {} {} {}".format(self.host,self.stresser,self.parms), stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)      
      out,err = ps.communicate()
      if err == "":
	self._return = 0
      else:
	logging.info("thread stress error {}".format(err))
	self._return = -1
      logging.info("thread stress endtime %d",int(time.time()*1000.0))
   def join(self):
      Thread.join(self)
      return self._return

class threadWorkload(threading.Thread):
   def __init__(self):
      threading.Thread.__init__(self)
   def run(self):
      logging.info("thread workload starttime %d",int(time.time()*1000.0))
      #subprocess.call("./"+WORKLOAD)
      ps = subprocess.Popen("./"+WORKLOAD, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)      
      out,err = ps.communicate()
      logging.info("thread workload endtime %d",int(time.time()*1000.0))
      if out.strip().endswith("... end of run"):
	self._return = 0
      else:
	self._return = -1
   def join(self):
      Thread.join(self)
      return self._return
def getLastSkydiveES():
    es = Elasticsearch(
      	[ELASTICSEARCH_IP],scheme='http',port=ELASTICSEARCH_PORT,
    )
    res = es.search(doc_type='flow',body={
		"size": 0,
          	"aggs": {"max_source.Metric.Last": {
		            "max": {"field":'Metric.Last'}
                        }
                 }
            })
    
    try:
    	newestMetricLast = int(res['aggregations']['max_source.Metric.Last']['value'])
    except:
	newestMetricLast = 0
    logging.info("Skydive newest Metric.Last {}".format(newestMetricLast))
    return(newestMetricLast)
def getLastJmeterInfluxDB():
    client=InfluxDBClient(host=u'9.148.244.43', port=31701, database=u'jmeter',  proxies=None)
    query="SELECT meanAT FROM jmeter"
    points = client.query(query, chunked=True, chunk_size=10000,epoch='ms').get_points()
    df = pd.DataFrame(points)
    newestTime = df['time'].max()
    logging.info("JmeterInfluxDB newest time {}".format(newestTime))
    return(newestTime)


if __name__ == "__main__":
    fn = WORKLOAD_STRESS_INDEX
    fieldnames = ['workload','stress_test','begin','end','qosBefore','qosAfter','qoeBefore','qoeAfter','elapse_time']
    if os.path.exists(fn):
        csvfile = open(fn,'a')
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    else:
        csvfile = open(fn,'w')
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
    for i in range(0,ITERATIONS):
        logging.info("iteration %d",i)
        logging.info("with no_stress")
        # Run workload without stress
        workload = threadWorkload()
	qosBefore = getLastSkydiveES()
	qoeBefore = getLastJmeterInfluxDB()
        begin_test = int(time.time()*1000.0)
        workload.start()
        workload.join()
	qosAfter = getLastSkydiveES()
	qoeAfter = getLastJmeterInfluxDB()
        end_test = int(time.time()*1000.0)
        elapse_time = end_test - begin_test
        logging.info("stress %s elapse_time %d","no_stress",elapse_time)
        writer.writerow({'workload':WORKLOAD_LABEL,'stress_test':'no_stress','begin':begin_test,'end':end_test,'qosBefore':qosBefore,'qosAfter':qosAfter,'qoeBefore':qoeBefore,'qoeAfter':qoeAfter,'elapse_time':elapse_time})
        csvfile.flush()

        # Run all test files for the current installed SkyDive chart
	for stresser_file in STRESSERS:
              	logging.info("with stress %s",stresser_file)
		filename_suffix = stresser_file.replace(".yaml","") 
		stresser_specs = yaml.load(open(stresser_file))
		print(stresser_specs)
		qosBefore = getLastSkydiveES()
		stressers = []
		for stresser_vars in stresser_specs:
			stresser = stresser_vars.get('stresser','iperf3')
			host = stresser_vars.get('host','localhost')
			parms = stresser_vars.get('parms','')
			logging.info("stresser {} host {} parms {}".format(stresser, host, parms))
 		  	stress = threadStress(stresser,host,parms)
		  	stress.start()
			#stressers.append[stress]
                sleep(30)
		qoeBefore = getLastJmeterInfluxDB()
		workload = threadWorkload()		
                begin_test = int(time.time()*1000.0)		
        	workload.start()
                logging.info("wait for workload to end")
	        workloadResponse = workload.join()
		logging.info("workloadResponse {}".format(workloadResponse))
		end_test = int(time.time()*1000.0)
	        elapse_time = end_test - begin_test
	        logging.info("stress %s elapse_time %d",filename_suffix,elapse_time)
                logging.info("kill stressers")
		stressEndedEarly = True
		for stresser_vars in stresser_specs:
			stresser = stresser_vars.get('stresser','iperf3')
 			host = stresser_vars.get('host','localhost')
      		  	ps = subprocess.Popen("ssh {} killall {}".format(host,stresser), stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)      
      		  	out,err = ps.communicate()
		  	print("out",out)
		  	print("err",err)                  	
		  	if out == '':
				logging.info("kill succeeded")
				stressEndedEarly = False

		print('stressEndedEarly is {}'.format(stressEndedEarly))
		logging.info("wait for stress to end")
		stressResponse = 0
		for stress in stressers:
                	stressResponse = stressResponse + stress.join()
                logging.info("stress end")
		qoeAfter = getLastJmeterInfluxDB()
		qosAfter = getLastSkydiveES()
		if workloadResponse == 0 and not stressEndedEarly:
                  writer.writerow({'workload':WORKLOAD_LABEL,'stress_test':filename_suffix,'begin':begin_test,'end':end_test,'qosBefore':qosBefore,'qosAfter':qosAfter,'qoeBefore':qoeBefore,'qoeAfter':qoeAfter,'elapse_time':elapse_time})
        	csvfile.flush()

