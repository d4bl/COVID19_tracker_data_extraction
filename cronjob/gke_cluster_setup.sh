
'''
Google Kubernetes Engine setup script for Data 4 Black Lives COVID-19 Data Tracker
Author: Sydeaka Watson
Updated: July 2020

This script sets up creates the cluster on Google Kubernetes Engine and runs the CronJob.

This script is best run on Google Cloud Shell via the terminal provided by Google Cloud in 
a web browser. Google Cloud Shell has the environment set up correctly, so there is minimal risk of 
system-to-system setup issues.

The Google Cloud Shell is available in the upper right corner of the Google Cloud Console
The icon looks like a small white box with `>_` inside. 
* Link to Google Cloud Console: https://console.cloud.google.com

Detailed instructions for setting up a CronJob on GKE: 
  https://cloud.google.com/kubernetes-engine/docs/how-to/cronjobs#using-gcloud-config

Other helpful links:
- https://stackoverflow.com/questions/58710541/how-to-run-shell-script-using-cronjobs-in-kubernetes
- https://kubernetes.io/docs/tasks/job/automated-tasks-with-cron-jobs/#job-template
'''


##########################################################################################
# To ensure that you are accessing the appropriate resources,
#  we recommend that you run these every time you log into Google Cloud Shell.

# Set environment variables
export cluster_name=d4bl-cluster
export project_id=d4bl-covid19-282714
export server_name=d4bl-server
export repo_name=d4bl-covid19-github
export container_link=gcr.io/$project_id/$repo_name:latest
export exposed_port=8080
export zone=us-central1
export location_flag=c
export locations=${zone}-${location_flag}
# export folder_name=d4bl-covid19-github

# Set project ID and zone
gcloud config set project $project_id
gcloud config set compute/zone $zone
##########################################################################################







##########################################################################################
# Create the cronjob.yaml file
# After creating this once, you won't have to do it again. 
# This file will persist and will continue to be available in future connections to Google Cloud Shell.


echo """
# CronJob configuration script for Data 4 Black Lives COVID-19 Data Tracker
# Author: Sydeaka Watson
# Updated: July 2020
# 
# This is the cronjob.yaml file that configures the CronJob for use on Googles Kubernetes Engine.
# As indicated in the schedule parameter setting below, the job is set to run every day 
#   at 4:45, 10:45, 16:45, and 22:45 in the Etc/UTC time zone.
#
# Default time zone is Etc/UTC (i.e., +0:00). 
# For reference, America/Chicago time zone is -5:00 or -6:00 (daylight savings time)

apiVersion: batch/v1beta1
kind: CronJob
metadata:
  name: d4bl
spec:
  schedule: "45 4,10,16,22 * * *"
  startingDeadlineSeconds: 100
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: d4bl
            image: gcr.io/d4bl-covid19-282714/d4bl-covid19-github:latest
            command: [ "/bin/sh" ]
            args: [ "/execute.sh" ]
            ports:
              - containerPort: 8080
          restartPolicy: Never
          """ > cronjob.yaml





##########################################################################################


"""
Start a cluster: 

Every Google Cloud Platform account gets one free cluster. To avoid unexpected billing for 
extra clusters, make sure you don't have any other clusters running before starting a new 
one. To check, make sure that this link shows no clusters running.  
You may have to click a link to select the D4BL project before viewing the clusters.
 https://console.cloud.google.com/compute/instances
 
This code creates one single-node cluster with replication in 3 locations within the zone 
specified above. If one location is unavailable for whatever reason, the job can continue 
to run in other locations that are available and are operating normally.
"""


# Start the cluster, get credentials, and optionally view cluster details
# References:
#  - https://cloud.google.com/sdk/gcloud/reference/container/clusters/create
#  - https://cloud.google.com/sdk/gcloud/reference/container/clusters/get-credentials
#  - https://cloud.google.com/sdk/gcloud/reference/container/clusters/describe
gcloud container clusters create $cluster_name --num-nodes=1 --zone $zone --node-locations $locations
gcloud container clusters get-credentials $cluster_name
gcloud container clusters describe $cluster_name

# Set up the cronjob called `d4bl` using the YAML file created above
kubectl apply -f cronjob.yaml 

# View the configuration of the `d4bl` cronjob
kubectl describe cronjob d4bl

# View the cronjob activity
kubectl get pods

##########################################################################################
# Other helpful commands:

# Check the logs of a particular pod (replace <pod_name> with the name of the pod)
#   kubectl logs <pod_name>

# Delete the cronjob
#   kubectl delete cronjob d4bl

# Delete the cluster
#   gcloud container clusters delete $cluster_name --zone $zone
