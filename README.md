# Taxirides data ETL pipeline with MageAI on GCP 

## Description: 
Buiding an ETL pipeline to transform and analyze taxi rides data using Google Cloud Platform

## Technology used:
- Cloud: GCP
- Storage: Cloud Storage
- Data warehouse: BigQuery
- ETL: Mage AI
- Scripts: Python
- Environment: Jupyter Notebook 
- Vizualization: Looker Studio

## Data source: 
- Taxi rides trip data from NYC TLC Trip record data
- Format: csv
  
## Process summary:
- Engineered a data pipeline on Google Cloud to ingest 100K taxi ride records from json files
- The architecture involved designing a dimensional data model using star schema on Lucid Charts
- Utilized Jupyter Notebook for data cleansing and exploration using pandas to gain insights
- Orchestrated transformation and loading into BigQuery with Mage for efficient data warehousing
- Visualized key ride analytics on a Looker Studio dashboard

## Resources:
- https://www.mage.ai/
- https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page    
