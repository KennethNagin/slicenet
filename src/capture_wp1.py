from skydive.rest.client import RESTClient
import yaml
conf_vars = yaml.load(open('tests_conf.yaml'))
SKYDIVE_IP=conf_vars.get('skydive_ip', '9.148.244.26')
SKYDIVE_PORT=conf_vars.get('skydive_port', '30777')
restclient = RESTClient(SKYDIVE_IP+":"+SKYDIVE_PORT)
captures = restclient.capture_list()
print("capture before")
for capture in captures:
        print("capture", capture)

restclient.capture_create("G.V().Has('Manager', NE('k8s'),'Docker.Labels.app', Regex('.*wordpress.*'),'Docker.Labels.tier', Regex('frontend')).Both().Out('Name','eth0')")
print("captures after")
captures = restclient.capture_list()
for capture in captures:
	print("capture", capture)
