#
# oci-compute-control-python version 1.0.
#
# Copyright (c) 2020 Oracle, Inc.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
#

import io
import os
import sys
import stat
import subprocess
import json
import oci
import oci.object_storage
from fdk import response
import paramiko
import time
import requests

# Parameters
instance_ocid = "ocid1.instance.oc1.iad.anuwcljspedlk6icqae4wgliap5lzx6o4ao25s3xpsj4nb6fenlho3i3bypq"



def run_scrapers():
    print('Get the auth key')
    resp_obj = get_object('just-another-bucket', 'covid19trackerkey.key')
    file_like_obj = io.StringIO(resp_obj['content'])
    pkey_obj = paramiko.rsakey.RSAKey(file_obj=file_like_obj)
    
    print('Establish SSH connection')
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect('132.145.200.60', username='ubuntu', pkey=pkey_obj)
    
    print('Prepare the command to be executed')
    SCRAPER_FILE='/home/ubuntu/COVID19_tracker_data_extraction/docker/run-scraper.sh'
    cmd_to_execute=". {}".format(SCRAPER_FILE)
    #cmd_to_execute="echo $HOME there is no place like home"
    
    print('Run the scraper')
    start = time.time()
    try:
        ssh_stdin, ssh_stdout, ssh_stderr = client.exec_command(cmd_to_execute)
        stdout = ssh_stdout.readlines()
    except Exception as ex:
        stdout = 'Exception occured: {}'.format(str(ex))
    
    end = time.time()
    elapsed_minutes = (end - start) / 60.0
    print('Success! Process completed in {} minutes'.format(elapsed_minutes))
    return elapsed_minutes, stdout

def xxxxxrun_scrapers():
    ex = 'success'
    try:
        # SSH into  the VM and execute  command
        ssh = paramiko.SSHClient()
        ssh.connect('132.145.200.60', username=ubuntu, password=token)
        SCRAPER_FILE='/home/ubuntu/COVID19_tracker_data_extraction/docker/run-scraper.sh'
        cmd_to_execute=". {}".format(SCRAPER_FILE)
        ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd_to_execute)
        # Wait for it to finish
        n = 1
        while not ssh.exit_status_ready()  and  n <= 500:
            time.sleep(1)
            stdout = ssh.makefile("rb")
            output = stdout.readlines()
            n  += 1
    except Exception as ex:
        ssh_stdin, ssh_stdout, ssh_stderr, stdout, output = 'null', 'null', 'null', 'null', 'null'
        ex = str(ex)
    
    return ssh_stdin, ssh_stdout, ssh_stderr, stdout, output, ex 


def get_object(bucketName, objectName):
    signer = oci.auth.signers.get_resource_principals_signer()
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    namespace = client.get_namespace().data
    try:
        print("Searching for bucket and object", flush=True)
        object = client.get_object(namespace, bucketName, objectName)
        print("found object", flush=True)
        if object.status == 200:
            print("Success: The object " + objectName + " was retrieved with the content: " + object.data.text, flush=True)
            message = object.data.text
        else:
            message = "Failed: The object " + objectName + " could not be retrieved."
    except Exception as e:
        message = "Failed: " + str(e.message)
    return { "content": message }


def start_or_stop_vm(compute_client, instance_id, action, wait_secs=10, num_tries=20):
    
    # Get initial status
    status = instance_status(compute_client, instance_id)
    
    intermediate_states = ['STOPPING', 'STARTING']
    
    if status in intermediate_states:
        vm_resp = 'Instance is {}... please try again.'.format(status)
        return vm_resp
    
    # Set state parameters
    if action == 'stop':
        ## For stopping a running instance
        initial_state = 'RUNNING'
        desired_state = 'STOPPED'
    elif action == 'start':
        ## For starting a stopped instance
        initial_state = 'STOPPED'
        desired_state = 'RUNNING'
    else:
        return 'Invalid action'
    
    
    # Perform the VM action
    if status == desired_state:
            return 'Instance already {0}'.format(status)
    elif status != initial_state:
            return 'Invalid instance state'
    else:
        # Kick off the action
        if action == 'stop':
            vm_resp = instance_stop(compute_client, instance_id)
        elif action == 'start':
            vm_resp = instance_start(compute_client, instance_id)
        
        # While loop
        n = 1
        while n <= num_tries and status != desired_state:
            time.sleep(wait_secs)
            status = instance_status(compute_client, instance_id)
            n += 1
    
    return vm_resp







def instance_status(compute_client, instance_id):
    return compute_client.get_instance(instance_id).data.lifecycle_state

def instance_start(compute_client, instance_id):
    print('Starting Instance: {}'.format(instance_id))
    try:
        if instance_status(compute_client, instance_id) in 'STOPPED':
            try:
                resp = compute_client.instance_action(instance_id, 'START')
                print('Start response code: {0}'.format(resp.status))
            except oci.exceptions.ServiceError as e:
                print('Starting instance failed. {0}' .format(e))
                raise
        else:
            print('The instance was in the incorrect state to start' .format(instance_id))
            raise
    except oci.exceptions.ServiceError as e:
        print('Starting instance failed. {0}'.format(e))
        raise
    print('Started Instance: {}'.format(instance_id))
    return instance_status(compute_client, instance_id)

def instance_stop(compute_client, instance_id):
    print('Stopping Instance: {}'.format(instance_id))
    try:
        if instance_status(compute_client, instance_id) in 'RUNNING':
            try:
                resp = compute_client.instance_action(instance_id, 'STOP')
                print('Stop response code: {0}'.format(resp.status))
            except oci.exceptions.ServiceError as e:
                print('Stopping instance failed. {0}' .format(e))
                raise
        else:
            print('The instance was in the incorrect state to stop' .format(instance_id))
            raise
    except oci.exceptions.ServiceError as e:
        print('Stopping instance failed. {0}'.format(e))
        raise
    print('Stopped Instance: {}'.format(instance_id))
    return instance_status(compute_client, instance_id)


def download_file_bucket(filename):
    try:
        # Download the object from storage bucket
        resp_obj = get_object('just-another-bucket', filename)
        try:
            # Write contents to a file
            new_filename = './' + filename + '.txt'
            text_file = open(new_filename, "w")
            num_chars = text_file.write(resp_obj['content'])
            text_file.close()
            ret = "File '{}' successfully downloaded.".format(filename)
        except Exception as ex1:
            ret = 'ex1:' + str(ex1)
    except Exception as ex:
        ret = 'ex:' + str(ex)
        
    return ret


def download_file_requests(filename):
    try:
        par_url = 'https://objectstorage.us-ashburn-1.oraclecloud.com/p/JPuAPaexFxkU_9w4VvfQ3qEEa799e-UVBIkFyuovcKg/n/idqkftjee5oj/b/just-another-bucket/o/covid19trackerkey.key'
        r = requests.get(par_url)
        with open('./' + filename, 'wb') as f:
            f.write(r.content)
        
        ret = "File '{}' successfully downloaded.".format(filename)
    except Exception as ex:
        ret = str(ex)
    
    return ret


def handler(ctx, data: io.BytesIO=None):
    
    signer = oci.auth.signers.get_resource_principals_signer()
    compute_client = oci.core.ComputeClient(config={}, signer=signer)
    
    # Check status, confirm that it is "Stopped"
    status = instance_status(compute_client, instance_ocid)
    
    # Start the VM
    try:
        x + 2
        vm_resp_start = start_or_stop_vm(compute_client, instance_ocid, 'start')
    except Exception as ex:
        vm_resp_start = str(ex)
    
    time.sleep(5)
    
    # Run the scrapers
    try:
        elapsed_minutes, stdout = run_scrapers()
        #elapsed_minutes, stdout = '1 min', 'All is well'
    except Exception as ex:
        elapsed_minutes, stdout = '0 min', 'Exception: ' + str(ex)
    
    
    # Stop the VM
    try:
        x + 2
        vm_resp_stop = start_or_stop_vm(compute_client, instance_ocid, 'stop')
    except Exception as ex2:
        vm_resp_stop = str(ex2)
    
    
    output_dict = { 
    "vm_resp_start": "{0}".format(vm_resp_start),
    "vm_resp_stop": "{0}".format(vm_resp_stop),
    "final status": "{0}".format(status),
    "elapsed_minutes": elapsed_minutes,
    "stdout": stdout
    }
    
    return response.Response(
        ctx, 
        response_data=json.dumps(output_dict),
        headers={"Content-Type": "application/json"}
    )
