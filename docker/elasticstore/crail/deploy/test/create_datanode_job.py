from os import path
import subprocess
import sys
import time
import json
import requests

import yaml

from kubernetes import client, config

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
       job['spec']['template']['spec']['nodeSelector']['kubernetes.io/hostname'] = node_affinity

    k8s_beta = client.BatchV1Api()
    resp = k8s_beta.create_namespaced_job(body=job, namespace="crail")
    print("Job created. status='%s'" % str(resp.status))

def stop_datanode_job(name):

    # (forcefully) terminates running pod of a datanode

    yaml_file = "../yamls/crail-datanode-job-test.yaml"
    
    config.load_kube_config()
    k8s_beta = client.BatchV1Api()
    
    body = client.V1DeleteOptions(propagation_policy='Background')
    resp = k8s_beta.delete_namespaced_job(name=name, body=body, namespace="crail", propagation_policy="Foreground")
    print("Job deleted. status='%s'" % str(resp.status))

def notify_datanode(name):
    # this method notifies a datanode running in a pod that it will be removed soon

    config.load_kube_config()
    api_instance = client.CoreV1Api()
    res = api_instance.list_namespaced_pod(namespace='crail', label_selector='job-name={}'.format(name))
    
    # format when using flannel
    # datanode_ip = json.dumps(res.items[0].to_dict()['metadata']['managed_fields'][1]['fields_v1']['f:status']['f:podIPs']).split("\\")[3][1:]

    # format when using calico
    datanode_ip = json.dumps(res.items[0].to_dict()['metadata']['managed_fields'][2]['fields_v1']['f:status']['f:podIPs']).split("\\")[3][1:]

    svc = api_instance.list_namespaced_service(namespace='crail', label_selector='run={}'.format('crail-relocator'))

    if len(svc.items) == 0:
        print("Error: Could not find relocator service. Make sure it is running on the cluster ... ")
        return

    relocator_ip = svc.items[0].to_dict()['spec']['cluster_ip']

    print(relocator_ip)

    server = 'http://' + relocator_ip + ':8765/'
    
    r = requests.post(server+'remove',data={'ip': datanode_ip, 'port': 50020})

    if r.status_code == 200:
        print("Removed datanode at " + datanode_ip)
    else:
        print("Error occurred when trying to remove datanode at " + datanode_ip)

def main():
    
    #time.sleep(30)

    start_datanode_job("tcp-testnode-1", node_affinity='flex02')
    start_datanode_job("tcp-testnode-2", node_affinity='flex02')
    start_datanode_job("tcp-testnode-3", node_affinity='flex02')
    start_datanode_job("tcp-testnode-4", node_affinity='flex02')
    
    time.sleep(60)

    #notify_datanode("tcp-testnode-1")
    notify_datanode("tcp-testnode-2")
    notify_datanode("tcp-testnode-3")
    #notify_datanode("tcp-testnode-4")

    time.sleep(120)

    start_datanode_job("tcp-testnode-2", node_affinity='flex02')
    start_datanode_job("tcp-testnode-3", node_affinity='flex02')
    #start_datanode_job("tcp-testnode-4", node_affinity='flex01')

    #start_datanode_job("tcp-testnode-1", node_affinity='flex01')
    #start_datanode_job("tcp-testnode-2", node_affinity='flex01')
    #start_datanode_job("tcp-testnode-3", node_affinity='flex01')
    #start_datanode_job("tcp-testnode-4", node_affinity='flex01')
    #start_datanode_job("tcp-testnode-5", node_affinity='flex01')
    #start_datanode_job("tcp-testnode-6", node_affinity='flex01')

    #start_datanode_job("tcp-testnode-1", node_affinity='flex02')
    #start_datanode_job("tcp-testnode-2", node_affinity='flex02')
    #start_datanode_job("tcp-testnode-3", node_affinity='flex02')
    #start_datanode_job("tcp-testnode-4", node_affinity='flex02')
    #start_datanode_job("tcp-testnode-5", node_affinity='flex02')
    #start_datanode_job("tcp-testnode-6", node_affinity='flex02')
    #start_datanode_job("tcp-testnode-7", node_affinity='flex02')
    #start_datanode_job("tcp-testnode-8")
    #start_datanode_job("tcp-testnode-9")
    #start_datanode_job("tcp-testnode-10")
    #start_datanode_job("tcp-testnode-11")
    #start_datanode_job("tcp-testnode-12")
    #start_datanode_job("tcp-testnode-13")
    #start_datanode_job("tcp-testnode-14")
    #start_datanode_job("tcp-testnode-15")
    #start_datanode_job("tcp-testnode-16")
    
    while False:
        time.sleep(60)
        notify_datanode("tcp-testnode-2")
        time.sleep(60)
        start_datanode_job("tcp-testnode-2")
        time.sleep(60)
        notify_datanode("tcp-testnode-1")
        time.sleep(60)
        start_datanode_job("tcp-testnode-1")

    
    
    #start_datanode_job("tcp-testnode-17", running_datanodes)
    #start_datanode_job("tcp-node-2")

    #notify_datanode("tcp-testnode-1")
    


if __name__ == '__main__':
    main()
