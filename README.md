# NFL data engineering project documentation

## Overview

The goal of this project is to apply data engineering (DE) concepts to orchestrate scraping and upload of NFL data to Google Cloud Platform (GCP) and perform some high level transformation and visualization with dbt and Tableau, respectively. The project uses an "extract-load-transform" (ELT) philosophy, where data is loaded into and then subsequently transformed within the DWH rather than between the 'extract' and 'load' steps (ETL).

The data were scraped using [nflfastpy](https://github.com/fantasydatapros/nflfastpy).

A data dictionary for play-by-play data can be found at the [nflverse website](https://nflreadr.nflverse.com/articles/dictionary_pbp.html).

I've presented a small overview of some of the data analysis in a [Tableau Public dashboard](https://public.tableau.com/app/profile/jacob.h.cipar/viz/ETLGCSprojectNFLFantasy/Dashboard1#2) where you can look at fantasy points scored vs. target share and overall player fantasy football performance. This dashboard highlights that volume is king (with some outliers) when it comes to how well a player will perform in fantasy football (at least, in PPR leagues, where a point is awarded for each reception).


## Pipeline technologies used:

![Pipeline flowchart diagram](/images/NFL_DE_flowchart_diagram.png "Data pipeline visualization")

The pipeline was built to aggregate NFL play-by-play (pbp), roster, and schedule data dating back to 1999, as well as team injury data (back to 2009).

Apache Airflow is used to orchestrate batch data ingestion into the Google Cloud Storage (GCS) data lake and Google BigQuery (BQ) data warehouse. dbt is used to perform transformations within BQ where core metrics for game-by-game performance of NFL wide receivers are calculated and used as a final 'data mart' layer for Tableau.
All infrastracture for GCP is deployed with Terraform. Airflow is deployed on a Docker image using docker-compose.

## Batch data ingestion

Data is ingested in [Airflow](/airflow/) using the nflfastpy package. Data for each category (pbp, roster, schedule, and injury) is downloaded to the local machine on a per-year basis, formatted to .parquet, uploaded to GCS. The local downloaded files are removed after uploading to GCS with a final DAG task.

A separate set of DAGs creates external tables in BQ for each data type. Note — because of issues with these files formatted as .parquet, partitioning in BQ by the created game_timestamp column is not supported. A future update will eliminate this bug by explicitly providing column types to the .parquet files.

The DAG tasks are currently set to run every year; this could be modified (in this case, to run every Tuesday at 7:00 AM from August-January) by using the following cron syntax for the schedule_interval argument: `00 7 * 8-12,1 2`. This could allow updating the dataset each week for NFL fantasy football analysis, for example.

## Data warehouse

Google BigQuery is used as the data warehouse. External tables are created using the 'separate set of DAGs' mentioned in the 'Batch data ingestion' section.

## Transformations

Data transformations are made with [dbt cloud](/dbt/). The primary dataset used is play-by-play data; a staging step is used to select columns not broken by the .parquet formatting. 

The primary transformation of interest for this project can be found in the dbt/models/core/facts_receivers.sql file. This transformation calculates receiving yards, touchdowns, number of receptions, and number of targets for each NFL receiver. It also uses window functions to calculate season average receiving yards/game and attempted passes per game on each receivers team (useful for calculating metrics like receiver target share).

## Dashboard/Visualization

A [dashboard](https://public.tableau.com/app/profile/jacob.h.cipar/viz/ETLGCSprojectNFLFantasy/Dashboard1#1) is available on Tableau Public to explore some of the transformations I prepared for Fantasy Football data viz.

![Alt text](/images/Dashboard_overview.png "Dashboard overview example")

This dashboard allows visualization of the distribution of week-to-week fantasy football points scored at the individual player and position levels, with controls to filter for specific seasons, weeks, and individual players. To select a particular season, use the slider or arrow buttons in the top left corner. The data displayed initially is for the entire season — click a week (1-18) in the barchart to see stats for that particular week:

![Alt text](/images/Dashboard_week_selection_example.gif "Dashboard overview example")

The barchart in the bottom left displaying the distribution of fantasy points scored is colored by player position; click a color to display just the stats corresponding to players with that position. You also can click a player in the bottom right scatterplot to show just the week-to-week fantasy points scored by that player in the top right:

![Alt text](/images/Dashboard_player_selection_example.gif "Dashboard example 2")

It's a but clumsy, but you also can filter for just a single player by name by searching for their name (first initial + last name) in the top left, and selecting the corresponding name that pops up below:

![Alt text](/images/Dashboard_player_selection_example_2.gif "Dashboard example 2")

This dataset and dashboard highlights the correlation between fantasy points scored and overall receiving targets. One actionable takeaway from these data is that when considering who to draft or play on any week as part of your NFL fantasy football team, choose players that are likely to get a lot of passes thrown at them! Especially when deciding between receivers on a week-to-week basis, consider playing receivers that might be more likely to have a higher target share for that particular week (e.g., if the WR1 on their team is injured that week).

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

## Next steps/future improvements

- Improve project documentation
- Fix .parquet column bugs in DWH by explicitly providing column types in data ingestion DAG
- Calculate additional position stats in dbt (e.g., rushing yards and touchdowns, passing yards and touchdowns, special teams, defensive metrics)
