# Data Engineering Capstone Project


## Problem description

We are tasked with finding crime patterns across one state in the US , for this, we need an open dataset that can help with a better understanding of the crimes of each location, I decided that pulling the data from the [Chicago Data Portal](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2) which reports the crimes reported in the City of Chicago on a daily basis starting from 2001 to Present . 

This could serve us as a good starting ground to make further analysis , the dataset is composed of 26 different Columns , including the Latitude and Longitude coordinates , as well as the Geo-location point.


**Note**: To use this data , we are required to include this disclaimer:

>This site provides applications using data that has been modified for use from its original source, http://www.cityofchicago.org/, the official website of the City of Chicago. The City of Chicago makes no claims as to the content, accuracy, timeliness, or completeness of any of the data provided at this site. The data provided at this site is subject to change at any time. It is understood that the data provided at this site is being used at one’s own risk.

## Project architecture


## Data Pipeline

The data is obtained with the `GCS_BQ_DAG` dag which makes use of the SODA API, with this API we can obtain the records of crimes by month and year, 

# Introduction


Requirements:




## To Reproduce the Project

Before reproducing the project, you need some requirements:

- **GCP** (Google Cloud Platform) account with billing enabled, using the 300$ trial is recommended.
- The Cloud SDK CLI tool for interacting with GCP projects , this can be helpful to manage all the services from the terminal.
  
You can create a project with GCP following the instructions of the course, I have included a script to facilitate this however, you will need to have the `gcloud cli ` installed , and you should be authenticated with `gcloud auth` , to create a project quickly , simply run:

```console
sh -c gcs_scripts/set_up.sh
```
Don't forget to add your intented credentials to `gcs_scriptss/variables.sh`



## Project Navigatioe:
You would need service account keys:
https://cloud.google.com/iam/docs/creating-managing-service-account-keys

Don't forget to assign the roles of :

## Architecture


The pipeline is defined as per the following architecture:

- We download the data per month and year , then we upload this data to a GCP Bucket (Data Lake)
- We transform this data and prepare with DBT to use it in a data warehouse.
- We crate a Dashboard that will display the relevant information required.



## Dashboard

The final Dashboard can be found here:

**Preview**: