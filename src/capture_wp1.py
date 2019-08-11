from skydive.rest.client import RESTClient
import yaml
conf_vars = yaml.load(open('tests_conf.yaml'))
SKYDIVE_IP=conf_vars.get('skydive_ip', '9.148.244.26')
SKYDIVE_PORT=conf_vars.get('skydive_port', '30777')
restclient = RESTClient(SKYDIVE_IP+":"+SKYDIVE_PORT)
#restclient = RESTClient("9.148.244.26:31020")
#restclient.capture_create("G.V().Has('TID','075f613c-d0b1-5993-550e-18e36983ddc1')")
#restclient.capture_create("G.V().Has('TID','8c451cd5-a37f-5e51-7d89-8c2cc2c5e88d')")
#restclient.capture_create("G.V().Has('TID','b40081de-0060-5816-54c4-c3907e470941')")

captures = restclient.capture_list()
print("captrue before")
for capture in captures:
        print("capture", capture)

restclient.capture_create("G.V().Has('Manager', NE('k8s'),'Docker.Labels.app', Regex('.*wordpress.*'),'Docker.Labels.tier', Regex('frontend')).Both().Out('Name','eth0')")
print("captures after")
captures = restclient.capture_list()
for capture in captures:
	print("capture", capture)
