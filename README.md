# Data Engineering Capstone Project


## Problem description

We are tasked with finding crime patterns across one state in the US , for this, we need an open dataset that can help with a better understanding of the crimes of each location, I decided that pulling the data from the [Chicago Data Portal](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2) which reports the crimes reported in the City of Chicago on a daily basis starting from 2001 to Present . 

This could serve us as a good starting ground to make further analysis , the dataset is composed of 26 different Columns , including the Latitude and Longitude coordinates , as well as the Geo-location point.


**Note**: To use this data , we are required to include this disclaimer:

>This site provides applications using data that has been modified for use from its original source, http://www.cityofchicago.org/, the official website of the City of Chicago. The City of Chicago makes no claims as to the content, accuracy, timeliness, or completeness of any of the data provided at this site. The data provided at this site is subject to change at any time. It is understood that the data provided at this site is being used at one’s own risk.

## Project architecture


## Data Pipeline


The data is obtained with the `data_ingestion_gcs.py` dag which makes use of the 

# Introduction


Requirements:


## Dataset Selected:






## Project Navigation:



## To Reproduce the Project

Before reproducing the project, you need some requirements:

- **GCP** (Google Cloud Platform) account with billing enabled, using the 300$ trial is recommended.
- The Cloud SDK CLI tool for interacting with GCP projects , this can be helpful to manage all the services from the terminal.
  

 **Billing**
 

Finally, I link the newly created project with my billing id, so that the bills come to me:
`gcloud alpha billing accounts projects link $PROJECT_ID --account-id=$ACCOUNT_ID`



