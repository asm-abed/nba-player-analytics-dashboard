# NBA PLAYER ANALYTICS DASHBOARD
## Overview

This repo is developed as a capstone project for DataTalks Club's Data Engineering Zoomcamp 2024. The purpose of this project is to apply all the concepts and tools that we have learned and create an end-to-end data pipeline. 
<br>
<br>
The objectives of this specific project are: 
<br>
   - to create end-to-end pipelines to store NBA player statistics data from 1997-present<br>
   - perform different transformations from this data and show it to a dashboard<br>
   - this project also utilizes a player point prediction model that calculates a predicted points a player may score on the following match based on historical data <br>

## Index
1. [**Dashboard**](https://github.com/asm-abed/nba-player-analytics-dashboard?tab=readme-ov-file#dashboard) <br>
2. [**Architecture**](https://github.com/asm-abed/nba-player-analytics-dashboard?tab=readme-ov-file#architecture) <br>
     2.1 [**Technologies Used**](https://github.com/asm-abed/nba-player-analytics-dashboard/blob/main/README.md#21-technologies-used) <br>
3. [**Dataset**](https://github.com/asm-abed/nba-player-analytics-dashboard/blob/main/README.md#dataset) <br>
     3.1 [**Kaggle NBA Database**](https://github.com/asm-abed/nba-player-analytics-dashboard/blob/main/README.md#31-kaggle-nba-data-) <br>
     3.2 [**nba_api data**](https://github.com/asm-abed/nba-player-analytics-dashboard/main/README.md#32-nba_api-data-) <br>
     3.3 [**Player Points Prediction Model**](https://github.com/asm-abed/nba-player-analytics-dashboard/blob/main/README.md#33-player-points-prediction-model-) <br>
4. [**Workflow Orchestration**](https://github.com/asm-abed/nba-player-analytics-dashboard/blob/main/README.md#workflow-orchestration) <br>
     4.1 [**mage-pipelines filing structure**](https://github.com/asm-abed/nba-player-analytics-dashboard/blob/main/README.md#41-mage-pipelines-filing-structure) <br>
5. [**Reproduce this project**](https://github.com/asm-abed/nba-player-analytics-dashboard/blob/main/README.md#reproduce-this-project) <br>
     5.1 [**Setting up Virtual Machine Environment in Google Cloud**](https://github.com/asm-abed/nba-player-analytics-dashboard/blob/main/README.md#51-setting-up-virtual-machine-environment-in-google-cloud) <br>
     5.2 [**Setup Service Account**](https://github.com/asm-abed/nba-player-analytics-dashboard/blob/main/README.md#52-setup-service-account) <br>
     5.3 [**Starting Mage Project**](https://github.com/asm-abed/nba-player-analytics-dashboard/blob/main/README.md#53-starting-mage-project) <br>
     5.4 [**Run Spark transformations**](https://github.com/asm-abed/nba-player-analytics-dashboard/blob/main/README.md#54-run-spark-transformations) <br>
     5.5 [**Partitions and Clusters**](https://github.com/asm-abed/nba-player-analytics-dashboard/blob/main/README.md#55-partitions-and-clusters) <br>
   


## Dashboard
You can acces the dashboard through this [**link**](https://lookerstudio.google.com/reporting/2af71728-04ed-40ea-89a0-9612950f50c6). 
<br>
### Page 1: Player Performance Dashboard
![DB Page 1](./misc/dashboard1.png?raw=true "Dasboard Page 1")

### Page 2: Player Season Performance and Prediction Table
![DB Page 2](./misc/dashboard2.png?raw=true "Dasboard Page 2")

## Architecture
![Project Archi](./misc/architecture.png?raw=true "Architecture")

### 2.1 Technologies Used
In the development of this project, the following tools are used:
  - Google Compute Engine as virtual machine in creating the pipelines
  - Docker to run mage
  - Mage as the main workflow orchestration tool
  - Google Cloud Storage for the datalake
  - Google BigQuery for the data warehouse
  - Apache Spark for transformations
  - Google Dataproc for submitting Spark jobs
  - Looker Studio for the data visualization

## Dataset
###   3.1 **Kaggle NBA Data** <br>
   For this project I used the NBA Boxscore Dataset downloaded from [**Kaggle**](https://www.kaggle.com/datasets/szymonjwiak/nba-traditional?select=traditional.csv). The dataset is in csv format which I uploaded to a dedicated GCS bucket. It contains player boxscore data from matches in 1997-2023.

###   3.2 **nba_api Data** <br>
   I used the python library nba_api to retrieve boxscore data from the current season which pulls real time data from stats.nba.com api, the same source the Kaggle data is from. You'll find the endpoints and functions of this library in this [**Github Repo**](https://github.com/swar/nba_api)

 ###  3.3 **Player Points Prediction Model** <br>
   I got this model from a study which you'll fine [**here**](https://courses.cs.washington.edu/courses/cse547/23wi/old_projects/23wi/NBA_Performance.pdf). I used the equation they created which the one below:
   ![formula](./misc/formula.png?raw=true "Prediction Formula")

## Workflow Orchestration
![ETL Pipelines](./misc/workflow.png?raw=true "ETL Pipelines")

I created two sets pipelines in Mage as shown, one to load from the Kaggle dataset (historical) and another to retrieve up-to-date data from nba_api and upload them all as partitioned parquet files in the GCS bucket. The pipeline for nba_api can be ran daily to retrieve the latest NBA match data. You can refer to this [**youtube tutorial**](https://www.youtube.com/watch?v=C0fNc8ZOpSI) on how to set instance schedules in Google Compute Engine for Mage pipelines. 

### 4.1 [mage-pipelines](./mage-pipelines) Filing Structure
Mage pipelines are usually classified into three parts: Data Loaders, Transformers, and Data Exporters. In this repo, these folders contain the python scripts that consist each of the pipelines. 

## Reproduce this project
If you want to reproduce this project, here is the laid out steps to do that. 

### 5.1 Setting up Virtual Machine Environment in Google Cloud
To setup your VM environment, you can follow this [**youtube tutorial**](https://www.youtube.com/watch?v=ae-CV2KfoN0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=14) by DataTalks Club

### 5.2 Setup Service Account
  - Go to IAM & Admin → Service Accounts on Google Cloud Console
  - Create Service Accounts
  - Add these roles:
     - Viewer
     - Storage Admin
     - Storage Object Admin
     - BigQuery Admin
   - Click Continue > Select Manage Keys
   - Select: Add Key & → Create new key → JSON → Create
   - Key will be downloaded onto local computer

### 5.3 Starting Mage Project
Clone the git repo by:

```bash
git clone https://github.com/asm-abed/nba-player-analytics-dashboard.git
```
Run the commands to start the project
```bash
cd nba-player-analytics-dashboard/mage-pipelines
docker-compose build
docker-compose up
```
Add your GCP Service Account file to mage-pipelines/gcp_srv/ folder. Go to io_config.yaml and update the name of the service account json file.  <br>
After then, mage should be up and functional. <br>

Run the nba_api pipelines so you'll have a change to play at some data. You can tweak the dates to manipulate how much data you can process. <br>
![Mage](./misc/mage.png?raw=true "Mage")
### 5.4 Run Spark transformations 
Setup Dataproc Cluster as laid out in this [youtube tutorial](https://www.youtube.com/watch?v=osAiAYahvh8&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=65)

Upload the python files to a GCS bucket. To Submit a job you can follow these snippets below:
![run Spark 1](./misc/runspark1.png?raw=true "run Spark 1")
![run Spark 2](./misc/runspark2.png?raw=true "run Spark 2")

### 5.5 Partitions and Clusters
To implement partitions and clusters, run the query file [partitions.sql](./bigquery-warehousing/nba_database_partitioning.sql) in BigQuery

 
