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

conf_vars = yaml.load(open('tests_conf.yaml'))
# Global variables
LOG_FILE_NAME = conf_vars.get('logFileName', "iperf_tests.log")
WORKLOAD_LABEL=conf_vars.get('workload_label', 'wp2.jmx')
ITERATIONS=conf_vars.get('iterations', 1)
WORKLOAD=conf_vars.get('workload', 'my_workload.sh')
STRESSERS=conf_vars.get('stressers',[])

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
   def __init__(self, stresser,hosts,parms):
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

if __name__ == "__main__":
    fn = 'workload_stress_begin_end.csv'
    fieldnames = ['workload','stress_test','begin','end','elapse_time']
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
        #workload = threadWorkload()
        #begin_test = int(time.time()*1000.0)
        #workload.start()
        #workload.join()
        #end_test = int(time.time()*1000.0)
        #elapse_time = end_test - begin_test
        #logging.info("stress %s elapse_time %d","no_stress",elapse_time)
        #writer.writerow({'workload':WORKLOAD_LABEL,'stress_test':'no_stress','begin':begin_test,'end':end_test,'elapse_time':elapse_time})
        #csvfile.flush()

        # Run all test files for the current installed SkyDive chart
	for stresser_file in STRESSERS:
              	logging.info("with stress %s",stresser_file) 
		stresser_vars = yaml.load(open(stresser_file))
		print(stresser_vars)
		stresser_app = stresser_vars.get('stresser','iperf3')
		stresser_hosts = stresser_vars.get('hosts',['localhost'])
		stresser_parms = stresser_vars.get('parms',[])
		filename_suffix = stresser_file.replace(".yaml","")
		
		logging.info("stresser {} hosts {} parms {}".format(stresser_app, stresser_hosts, stresser_parms))
                default_working_dir = os.getcwd()  # type: str
                #os.chdir(SCRIPT_PATH)
		#stress = threadStress(filename)
		stressers = []
		i = 0
		for host in stresser_hosts:		  		
		  stress = threadStress(stresser_app,host,stresser_parms[i])
		  stress.start()
		  i = i+1
		  stressers.append(stress)
                sleep(30)
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
		for host in stresser_hosts:
		  #subprocess.call("ssh {} killall {}".format		(host,stresser_app), shell=True)
      		  ps = subprocess.Popen("ssh {} killall {}".format		(host,stresser_app), stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)      
      		  out,err = ps.communicate()
		  logging.info("out",out)
		  logging.info("err",err)
                  stressEndedEarly = True
		  if out == '':
			logging.info("kill succeeded")
			stressEndedEarly = False
		print('stressEndedEarly is {}'.format(stressEndedEarly))
		logging.info("wait for stress to end")
		stressResponse = 0
		for stress in stressers:
                	stressResponse = stressResponse + stress.join()
                logging.info("stress end")
		if workloadResponse == 0 and not stressEndedEarly:
                  writer.writerow({'workload':WORKLOAD_LABEL,'stress_test':filename_suffix,'begin':begin_test,'end':end_test,'elapse_time':elapse_time})
        	csvfile.flush()
                #os.chdir(default_working_dir)

