import os
import subprocess
from time import sleep
import time
from interruptingcow import timeout 
import threading
from threading import Thread
import paramiko
import logging
import yaml
import mmap
import re
import json
from pandas.io.json import json_normalize
import csv
from skydive.rest.client import RESTClient
from elasticsearch import Elasticsearch
from influxdb import DataFrameClient, InfluxDBClient
import pandas as pd

conf_vars = yaml.load(open('tests_conf.yaml'))
# Global variables
LOG_FILE_NAME = conf_vars.get('logFileName', "iperf_tests.log")
WORKLOAD_LABEL=conf_vars.get('workload_label', 'wp4.jmx')
WORKLOAD_ARGS=conf_vars.get('workload_args', 'wp4.jmx')
ITERATIONS=conf_vars.get('iterations', 1)
WORKLOAD=conf_vars.get('workload', 'my_workload.sh')
STRESSERS=conf_vars.get('stressers',[])
ELASTICSEARCH_IP=conf_vars.get('elasticsearch_ip', '9.148.244.26')
ELASTICSEARCH_PORT=conf_vars.get('elasticsearch_port', '30777')
WORKLOAD_STRESS_INDEX=conf_vars.get('workload_stress_index', 'workload_stress_index.csv')
SKYDIVE_IP=conf_vars.get('skydive_ip', '9.148.244.26')
SKYDIVE_PORT=conf_vars.get('skydive_port', '30777')
SKYDIVEFLOWS_CSV=conf_vars.get('skydiveflows_csv', 'skydiveFlows.csv')
TIME_OUT=conf_vars.get('time_out',30)*60
NO_STRESS=conf_vars.get('no_stress',True)



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

fn = WORKLOAD_STRESS_INDEX
fieldnames = ['workload','stress_test','begin','end','qoeBefore','qoeAfter','elapse_time']
if os.path.exists(fn):
   csvfile = open(fn,'a')
   writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
else:
   csvfile = open(fn,'w')
   writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
   writer.writeheader()
if os.path.exists(SKYDIVEFLOWS_CSV):
   skydiveFrames = [pd.read_csv(SKYDIVEFLOWS_CSV)]
else:
   skydiveFrames = []



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
      ps = subprocess.Popen("./"+WORKLOAD+" "+WORKLOAD_ARGS, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)      
      out,err = ps.communicate()
      logging.info("thread workload endtime %d",int(time.time()*1000.0))
      if out.strip().endswith("... end of run"):
	self._return = 0
      else:
	self._return = -1
   def join(self):
      Thread.join(self)
      return self._return
class threadGetSkydiveFlows(threading.Thread):
   def __init__(self):
      threading.Thread.__init__(self)
      self.collectFlows = True
      self._return = (pd.DataFrame(),"")
   def run(self):
      logging.info("thread threadGetSkydiveFlows starttime %d",int(time.time()*1000.0))
      err = ""
      restclient = RESTClient(SKYDIVE_IP+":"+SKYDIVE_PORT)
      gremlinFlow = "G.Flows().Has('Application', 'TCP')"
      flows = restclient.lookup(gremlinFlow)
      dfOldFlows = json_normalize(flows)
      frames = []
      time_out = time.time() + TIME_OUT
      while (self.collectFlows) & (time.time() < time_out):
	    flows = restclient.lookup(gremlinFlow)
	    df = json_normalize(flows)
	    if (not df.empty) & (not dfOldFlows.empty):
	    	cond = df['UUID'].isin(dfOldFlows['UUID']) == True
	    	df.drop(df[cond].index, inplace = True)
	    if not df.empty:
		frames.append(df)
	    sleep(1)
      if time.time() >= time_out:
	 err = "Error: skydive time out"
	 logging.info(err)
         
      df = pd.DataFrame() 
      if len(frames) > 0:
        df = pd.concat(frames,sort=False)
        df = df.drop_duplicates()
        df = df.sort_values("Metric.Last",ascending=True)
        df = df.drop_duplicates(subset="UUID", keep='last')
      self._return = (df,err)
   def join(self):
      #sleep(30) 
      self.collectFlows = False 
      Thread.join(self)
      return self._return
# getLastSkydiveES is not used.  
# Its idea was to index skydive's elasticsearch before and after a workload.
# However skydive updates these values in place so it did not turn out to be
# a good indexing strategy.
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

def doSampleAndCollectData(stress_test):
	logging.info("doSampleAndCollectData {}".format(stress_test))
        skydiveFlows = threadGetSkydiveFlows()
	skydiveFlows.start()
        qoeBefore = getLastJmeterInfluxDB()
        workload = threadWorkload()	
        begin_test = int(time.time()*1000.0)
        workload.start()
        #workloadResponse = -1
        workloadResponse = workload.join()
	#workload.terminate()
	logging.info("workloadResponse {}".format(workloadResponse))
	end_test = int(time.time()*1000.0)
        elapse_time = end_test - begin_test
        logging.info("stress %s elapse_time %d",stress_test,elapse_time)
	#skydiveFlowsDf = pd.DataFrame()
	#err = "ERROR: time out"
	skydiveFlowsDf, err = skydiveFlows.join()
	#skydiveFlows.terminate()
        if err == "":
	  logging.info("workload flows {}".format(skydiveFlowsDf.shape[0]))
	  if skydiveFlowsDf.shape[0] > 0:	   
	    skydiveFlowsDf["begin"] = begin_test
	    skydiveFlowsDf['stress_test'] = stress_test
	    skydiveFlowsDf['workload'] = WORKLOAD_LABEL
	    skydiveFrames.append(skydiveFlowsDf)
          else:
            err = "skydiveFlowsDf is empty"
	else:
	  logging.info("ERROR: {}".format(err))	   
	qoeAfter = getLastJmeterInfluxDB()        
	if workloadResponse == 0 and err == "":
        	writer.writerow({'workload':WORKLOAD_LABEL,'stress_test':stress_test,'begin':begin_test,'end':end_test,'qoeBefore':qoeBefore,'qoeAfter':qoeAfter,'elapse_time':elapse_time})
        	csvfile.flush()



if __name__ == "__main__":

    for i in range(0,ITERATIONS):
        logging.info("iteration %d",i)        
        # Run workload without stress
	if NO_STRESS:
		doSampleAndCollectData('no_stress')
	for stresser_file in STRESSERS:
              	logging.info("with stress %s",stresser_file)
		filename_suffix = stresser_file.replace(".yaml","") 
		stresser_specs = yaml.load(open(stresser_file))
		print(stresser_specs)
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
		doSampleAndCollectData(filename_suffix)
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
		#stressResponse = 0
		for stress in stressers:
                	stressResponse = stressResponse + stress.join()
			#stress.terminate()
                logging.info("stress end")
	if len(skydiveFrames) > 0:
		skydiveDF = pd.concat(skydiveFrames,sort=False)
		print("concat skydiveDF shape",skydiveDF.shape)
		skydiveDF.to_csv(SKYDIVEFLOWS_CSV)

