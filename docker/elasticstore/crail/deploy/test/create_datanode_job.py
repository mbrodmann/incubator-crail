from os import path
import subprocess
import sys
import time
import json
import requests
import random
import yaml
import importlib

from kubernetes import client, config

def get_datanode_status(name):
    config.load_kube_config()
    api_instance = client.CoreV1Api()
    res = api_instance.list_namespaced_pod(namespace='crail', label_selector='job-name={}'.format(name))
    job_status = res.items[0].to_dict()['status']['phase']

    return job_status

def start_datanode_job(name, node_affinity=None):

    # first check whether there already is a job with the given name
    config.load_kube_config()
    api_instance = client.CoreV1Api()
    res = api_instance.list_namespaced_pod(namespace='crail', label_selector='job-name={}'.format(name))

    k8s_beta = client.BatchV1Api()

    if len(res.items) > 0:
        job_status = res.items[0].to_dict()['status']['phase']
        if job_status == 'Succeeded':
            body = client.V1DeleteOptions(propagation_policy='Background')
            resp = k8s_beta.delete_namespaced_job(name=name, body=body, namespace="crail", propagation_policy="Foreground")
        else:
            print("!!! Error: Tried to start new datanode-job, but previous job is still running !!!")


    yaml_file = "../yamls/crail-datanode-job-test.yaml"

    f = open(yaml_file)
    job = yaml.load(f)

    job.get('metadata')['name'] = name
    job['spec']['template']['metadata']['labels']['name'] = name

    if node_affinity != None:
       job['spec']['template']['spec']['nodeSelector']['deployment_role'] = node_affinity

    k8s_beta = client.BatchV1Api()
    resp = k8s_beta.create_namespaced_job(body=job, namespace="crail")
    print("Created job " + name)

def stop_datanode_job(name):

    # (forcefully) terminates running pod of a datanode

    yaml_file = "../yamls/crail-datanode-job-test.yaml"
    
    config.load_kube_config()
    k8s_beta = client.BatchV1Api()
    
    body = client.V1DeleteOptions(propagation_policy='Background')
    resp = k8s_beta.delete_namespaced_job(name=name, body=body, namespace="crail", propagation_policy="Foreground")
    print("Delete job " + name)

def notify_datanode(name):
    # this method notifies a datanode running in a pod that it will be removed soon

    config.load_kube_config()
    api_instance = client.CoreV1Api()
    res = api_instance.list_namespaced_pod(namespace='crail', label_selector='job-name={}'.format(name))
    
    # format when using flannel
    # datanode_ip = json.dumps(res.items[0].to_dict()['metadata']['managed_fields'][1]['fields_v1']['f:status']['f:podIPs']).split("\\")[3][1:]

    # format when using calico
    # datanode_ip = json.dumps(res.items[0].to_dict()['metadata']['managed_fields'][2]['fields_v1']['f:status']['f:podIPs']).split("\\")[3][1:]

    # format when using gke deployment
    datanode_ip = json.dumps(res.items[0].to_dict()['metadata']['managed_fields'][1]['fields_v1']['f:status']['f:podIPs']).split("\\")[3][1:]

    svc = api_instance.list_namespaced_service(namespace='crail', label_selector='run={}'.format('crail-relocator'))

    if len(svc.items) == 0:
        print("Error: Could not find relocator service. Make sure it is running on the cluster ... ")
        return

    # format for local kubernetes deployment with cluster ip
    # relocator_ip = svc.items[0].to_dict()['spec']['cluster_ip']
    # server = 'http://' + relocator_ip + ':8765/'

    # format for gke deployment using loadbalancer and external ip
    relocator_ip = svc.items[0].to_dict()['status']['load_balancer']['ingress'][0]['ip']
    relocator_port = str(svc.items[0].to_dict()['spec']['ports'][0]['port'])
    server = 'http://' + relocator_ip + ':' + relocator_port + '/'
    
    r = requests.post(server+'remove',data={'ip': datanode_ip, 'port': 50020})

    if r.status_code == 200:
        print("Removed datanode " + name)
    else:
        print("Error occurred when trying to remove datanode " + name)


def simulation():

    #machines = ["flex01", "flex02"]
    machines = ["flex02"]
    states = {}

    for machine in machines:
        states[machine] = [(0, 'Undefined'), (1, 'Undefined'), (2, 'Undefined'), (3, 'Undefined'), (4, 'Undefined')]

    dc_utilization = 0.1

    p_stop = dc_utilization
    p_start = 1-dc_utilization

    debug = True


    # Initial configuration start datanodes
    print("### Initializing System state ###")
    for machine in machines:
        for entry in states[machine]:

            slot = entry[0]
            state = entry[1]

            if state == 'Undefined':
                start_datanode_job("tcp-datanode-"+machine+"-"+str(slot), node_affinity=machine)
    print()
    print()


    step = 0
    while True:

        time.sleep(30)
        step = step+1

        ### gather states of each slot in each machine ###
        for machine in machines:
            for entry in states[machine]:

                slot = entry[0]
                current_state = get_datanode_status("tcp-datanode-"+machine+"-"+str(slot))
                states[machine][slot] = (slot, current_state)

        
        ### print system state iff specified
        print("### Step " + str(step) + " ###")
        print("### System State ###")
        if debug:
            for machine in machines:

                print(machine + "=[", end = '')

                for entry in states[machine]:
                    print("["+entry[1]+"]", end = '')
                
                print("]")
            print()


        ### update state for each slot in each machine ###
        print("### Changes to System State ###")
        for machine in machines:
            for entry in states[machine]:

                slot = entry[0]
                state = entry[1]

                rand = random.random()

                # check if running datanode container should be stopped
                if state == "Running":
                    if rand < p_stop:
                        notify_datanode("tcp-datanode-"+machine+"-"+str(slot))

                
                # check if stopped datanode container should be started again
                if state == "Succeeded":
                    if rand < p_start:
                        start_datanode_job("tcp-datanode-"+machine+"-"+str(slot), node_affinity=machine)
        print()
        print()

def dynamic():
    start_datanode_job("tcp-datanode-1", node_affinity='flex02')
    start_datanode_job("tcp-datanode-2", node_affinity='flex02')
    start_datanode_job("tcp-datanode-3", node_affinity='flex02')
    start_datanode_job("tcp-datanode-4", node_affinity='flex02')

    time.sleep(30)
    notify_datanode("tcp-datanode-2")
    time.sleep(30)
    notify_datanode("tcp-datanode-3")
    time.sleep(30)
    start_datanode_job("tcp-datanode-2", node_affinity='flex02')
    time.sleep(30)
    start_datanode_job("tcp-datanode-3", node_affinity='flex02')


def batch():
    start_datanode_job("tcp-datanode-1", node_affinity='flex02')
    start_datanode_job("tcp-datanode-2", node_affinity='flex02')
    start_datanode_job("tcp-datanode-3", node_affinity='flex02')
    start_datanode_job("tcp-datanode-4", node_affinity='flex02')
    
    time.sleep(100)

    notify_datanode("tcp-datanode-2")
    notify_datanode("tcp-datanode-3")

    #time.sleep(120)

    #start_datanode_job("tcp-datanode-2", node_affinity='flex01')
    #start_datanode_job("tcp-datanode-3", node_affinity='flex02')


def static():
    #start_datanode_job("tcp-datanode-1-flex01", node_affinity='flex01')
    #start_datanode_job("tcp-datanode-2-flex01", node_affinity='flex01')
    #start_datanode_job("tcp-datanode-3-flex01", node_affinity='flex01')
    #start_datanode_job("tcp-datanode-4-flex01", node_affinity='flex01')

    #start_datanode_job("tcp-datanode-1-flex02", node_affinity='flex02')
    #start_datanode_job("tcp-datanode-2-flex02", node_affinity='flex02')
    #start_datanode_job("tcp-datanode-3-flex02", node_affinity='flex02')
    #start_datanode_job("tcp-datanode-4-flex02", node_affinity='flex02')

    start_datanode_job("tcp-datanode-1", node_affinity='datanode-1')
    #start_datanode_job("tcp-datanode-2", node_affinity='datanode-2')
    #start_datanode_job("tcp-datanode-3", node_affinity='datanode-3')


def stop():
    #notify_datanode("tcp-datanode-1-flex01")
    #notify_datanode("tcp-datanode-2-flex01")
    #notify_datanode("tcp-datanode-3-flex01")
    #notify_datanode("tcp-datanode-4-flex01")

    #notify_datanode("tcp-datanode-1-flex02")
    #notify_datanode("tcp-datanode-2-flex02")
    #notify_datanode("tcp-datanode-3-flex02")
    #notify_datanode("tcp-datanode-4-flex02")

    notify_datanode("tcp-datanode-1")


def main():
    static()
    #stop()
    #simulation()

if __name__ == '__main__':
    main()
