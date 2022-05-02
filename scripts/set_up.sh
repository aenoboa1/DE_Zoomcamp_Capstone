#!/bin/bash

source ./variables.sh


# setup billing
gcloud alpha billing accounts list

# Download project generator script
curl https://raw.githubusercontent.com/GoogleCloudPlatform/training-data-analyst/master/blogs/gcloudprojects/create_projects.sh -o create_projects.sh
chmod +x create_projects.sh


./create_projects.sh 0X0X0X-0X0X0X-0X0X0X ${PROJECT_ID} ${PROJECT_EMAIL}
  