import requests
import os
import socket

from kubernetes import client,config

hostname = socket.gethostname()

# this can probably only be used when relocator service is started before datanode pod
# relocator_port = os.environ['CRAIL_RELOCATOR_PORT']
# relocator_address = relocator_port.split("//")[1]

config.load_kube_config()
api_instance = client.CoreV1Api()
svc = api_instance.list_namespaced_service(namespace='crail', label_selector='run={}'.format('crail-relocator'))
relocator_address = svc.items[0].to_dict()['spec']['cluster_ip']+':50000'

server = 'http://' + relocator_address + '/'

datanode_ip = os.environ['POD_IP']

# log that datanode pod is about to be terminated
msg = "Stopping " + hostname + " at " + os.popen('date').read()[:-1]
requests.post(server+'log', data=msg )

# start relocation
r = requests.post(server+'remove', data={'ip': datanode_ip, 'port': 50020})

if r.status_code == 200:
        print("Removed datanode")
else:
	print("Error occurred when trying to remove datanode")
