# Coopsight Backend 

Hi! Here is a brief overview of the basic structure, functions, and usage of Coopsight backend. Before you start developing, please review this doc carefully!

## Repository Structure

Here is an overview of the repository structure:

    Dockerfile 
    app.py 
    /test_suite 
    /backend_functions
	    hello_world.py
    /dev_functions
        *local development functions for debugging...
Currently, we are working with **app.py** and functions inside **backend_functions**. The idea is that all traffic initially gets routed in app.py, which executes functions in backend_functions and returns the results. 

 
## Creating a New Feature

To create a new feature. 

 1. Make a new file in backend_functions (this is the code for your feature)
 2. Import file into app.py
 3. Build a handle for the new feature (endpoint routing)

## Deploying to Kubernetes

**Caution: This deploys to production.**

Install Google Cloud on local terminal environment. Authenticate with gcloud init. 

    # Authenticate gke
    gcloud container clusters get-credentials coopsight-beta-cluster-2 --zone us-central1-a --project coopsightsoftware

    # Make sure to be in root directory of backend functions
    pwd
    
    # Submit docker build to gcloud
    gcloud builds submit --tag gcr.io/coopsightsoftware/backend-alpha

    # Update the deployment (run both to be safe)
    kubectl set image deployment/backend-alpha hello-app=gcr.io/coopsightsoftware/backend-alpha
    
    kubectl set image deployment/backend-alpha hello-app=gcr.io/coopsightsoftware/backend-alpha:latest



