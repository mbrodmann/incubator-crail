import requests
import os
import socket

hostname = socket.gethostname()

relocator_port = os.environ['CRAIL_RELOCATOR_PORT']
relocator_address = relocator_port.split("//")[1]
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
