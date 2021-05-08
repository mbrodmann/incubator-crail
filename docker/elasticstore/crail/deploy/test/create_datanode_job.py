from os import path
import subprocess
import sys
import time

import yaml

from kubernetes import client, config

def start_datanode_job(name):

    yaml_file = "../yamls/crail-datanode-job-test.yaml"

    config.load_kube_config()
    
    f = open(yaml_file)
    job = yaml.load(f)

    job.get('metadata')['name'] = name

    print(job)

    k8s_beta = client.BatchV1Api()
    resp = k8s_beta.create_namespaced_job(body=job, namespace="crail")
    print("Job created. status='%s'" % str(resp.status))

def stop_datanode_job(name):

    yaml_file = "../yamls/crail-datanode-job-test.yaml"
    parallelism = "1"
    cmd = ["./update_datanode_yaml.sh", name, parallelism, yaml_file]
    subprocess.Popen(cmd).wait()


    config.load_kube_config()
    k8s_beta = client.BatchV1Api()
    
    body = client.V1DeleteOptions(propagation_policy='Background')
    resp = k8s_beta.delete_namespaced_job(name=name, body=body, namespace="crail", propagation_policy="Foreground")
    print("Job deleted. status='%s'" % str(resp.status))

def main():
    
    start_datanode_job("tcp-node-1")
    start_datanode_job("tcp-node-2")
    time.sleep(60)
    stop_datanode_job("tcp-node-1")
    stop_datanode_job("tcp-node-2")
    


if __name__ == '__main__':
    main()
