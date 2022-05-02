# NFL data engineering project documentation

## Overview

The goal of this project is to apply data engineering (DE) concepts to orchestrate scraping and upload of NFL data to Google Cloud Platform (GCP) and perform some high level transformation and visualization with dbt and Google Data Studio, respectively.

The data were scraped using nflfastpy: https://github.com/fantasydatapros/nflfastpy

A data dictionary for play-by-play data can be found at: https://nflreadr.nflverse.com/articles/dictionary_pbp.html

![Alt text](/NFL_DE_flowchart_diagram.png "Data pipeline visualization")