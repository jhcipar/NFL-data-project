# NFL data engineering project documentation

## Overview

The goal of this project is to apply data engineering (DE) concepts to orchestrate scraping and upload of NFL data to Google Cloud Platform (GCP) and perform some high level transformation and visualization with dbt and Google Data Studio, respectively. The project uses an "extract-load-transform" (ELT) philosophy, where data is loaded 

The data were scraped using nflfastpy: https://github.com/fantasydatapros/nflfastpy

A data dictionary for play-by-play data can be found at: https://nflreadr.nflverse.com/articles/dictionary_pbp.html


## Pipeline technologies used:

![Alt text](/NFL_DE_flowchart_diagram.png "Data pipeline visualization")

The pipeline was built to aggregate NFL play-by-play (pbp), roster, and schedule data dating back to 1999, as well as team injury data (back to 2009).

Apache Airflow is used to orchestrate batch data ingestion into the Google Cloud Storage (GCS) data lake and Google BigQuery (BQ) data warehouse. dbt is used to perform transformations within BQ where core metrics for game-by-game performance of NFL wide receivers are calculated and used as a final 'data mart' layer for Google Data Studio.

All infrastracture for GCP is deployed with Terraform. Airflow is deployed on a Docker image using docker-compose.

## Batch data ingestion

Data is ingested in Airflow using the nflfastpy package. Data for each category (pbp, roster, schedule, and injury) is downloaded to the local machine on a per-year basis, formatted to .parquet, uploaded to GCS. The local downloaded files are removed after uploading to GCS with a final DAG task.

A separate set of DAGs creates external tables in BQ for each data type. Note — because of issues with these files formatted as .parquet, partitioning in BQ by the created game_timestamp column is not supported. A future update will eliminate this bug by explicitly providing column types to the .parquet files.

The DAG tasks are currently set to run every year; this could be modified (in this case, to run every Tuesday at 7:00 AM from August-January) by using the following cron syntax for the schedule_interval argument: '00 7 * 8-12,1 2'. This could allow updating the dataset each week for NFL fantasy football analysis, for example.

## Data warehouse

Google BQ is used as the datawarehouse. External tables are created using the 'separate set of DAGs' mentioned in the 'Batch data ingestion' section.

## Transformations

Data transformations are made with dbt cloud. The primary dataset used is play-by-play data; a staging step is used to select columns not broken by the .parquet formatting. 

The primary transformation of interest for this project can be found in the dbt/models/core/facts_receivers.sql file. This transformation calculates receiving yards, touchdowns, number of receptions, and number of targets for each NFL receiver. It also uses window functions to calculate season average receiving yards/game and attempted passes per game on each receivers team (useful for calculating metrics like receiver target share).

## Dashboard/Visualization

A dashboard is prepared in Google Data Studio to visualize some high-level stats, and can be found at: https://datastudio.google.com/reporting/d2d4243a-fe63-4180-bb6c-2b9d28b1c0f4

This dashboard allows visualization of total receiving yards per player, with controls to filter for specific seasons and individual receivers by name. For example, for fun, you could compare total receiving yards for all players that have "Smith" as part of their name. The dashboard also contains a time-series visualization for average yards/game over the course of a season or set of seasons.

## Reproducing this repo

- Prepare GCP service accounts with appropriate permissions for GCS and BQ
- Set up a VM on Google Cloud with docker, docker-compose, and git.
    - Clone this repo to the VM
    - Set up appropriate GCP credentials on the VM (including installing google cloud SDK and granting appropriate permissions)
- Change user-specific attributes in code:
    - In Terraform variables.tf change project id, region, and name of the GCS bucket to your GCS bucket
- In docker-compose.yaml change the environment variables for google cloud authentication, your GCP project ID, and the GCP bucket you're using
- Build image using docker build in /airflow/, and run airflow using docker-compose up
    - Forward port 8080 and connect to it on your local machine. Unpause the DAGs - they should catch up to the current year
- Grant dbt cloud the appropriate permissions for GCP and connect it to your DWH

## Next steps

-Improve project documentation
-Fix .parquet column bugs in DWH by explicitly providing column types in data ingestion DAG
-Calculate additional position stats in dbt (e.g., rushing yards and touchdowns, passing yards and touchdowns, special teams, defensive metrics)
