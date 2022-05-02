# NFL data engineering project documentation

## Overview

The goal of this project is to apply data engineering (DE) concepts to orchestrate scraping and upload of NFL data to Google Cloud Platform (GCP) and perform some high level transformation and visualization with dbt and Google Data Studio, respectively. The project uses an "extract-load-transform" (ELT) philosophy, where data is loaded 

The data were scraped using nflfastpy: https://github.com/fantasydatapros/nflfastpy

A data dictionary for play-by-play data can be found at: https://nflreadr.nflverse.com/articles/dictionary_pbp.html


## Pipeline technologies used:

![Alt text](/NFL_DE_flowchart_diagram.png "Data pipeline visualization")

The pipeline was built to aggregate NFL play-by-play (pbp), roster, and schedule data dating back to 1999, as well as team injury data (back to 2009).

Apache Airflow is used to orchestrate batch data ingestion into the Google Cloud Storage data lake and Google BigQuery data warehouse. dbt is used to perform transformations within BigQuery where core metrics for game-by-game performance of NFL wide receivers are calculated and used as a final 'data mart' layer for Google Data Studio.

## Batch data ingestion

Data is ingested in Airflow using the nflfastpy package over several DAGs

## Data warehouse

## Transformations

