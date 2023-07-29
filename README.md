Second data engineering project, using Python, Spark and Azure Databricks to create a data lakehouse architecture.

## Introduction

Some of the data is from Divvy, a bike sharing program in Chicago, Illinois. This data is anonymous, so Udacity provided some fake rider and account profiles to go along with fake payment data.

A rider can purchase a pass at a kiosk or use a mobile application to unlock a bike at stations around the city and use the bike for a specified amount of time. The bikes can be resturned to the same station or to another station.

## Project Description

Using the Azure tech stack, I wanted to create the following architecture:

![architecture diagram](https://github.com/chrisselig/azureDatabricksLakehouse/blob/main/80_imgs_for_readme/architecture.png)

In Azure, I uploaded csv files to the Databricks file system (DBFS).

The files were then moved from the DBFS to be stored as parquet files in the delta lake.

At this point, the files were moved to a bronze table storage in the delta lake.

Finally, the tables were cleaned and prepared for analysis by creating the dimension and fact tables below.

The transformations and creation of the tables were done via Python and Spark (sql)

## Original Model

![Original Relational Model](https://github.com/chrisselig/bikesharingDW/blob/main/80_imgs_for_readme/divvy-erd.png)

## New Star Schema
![Star Schema](https://github.com/chrisselig/azureDatabricksLakehouse/blob/main/80_imgs_for_readme/star_schema.png)


## Analysis - Questions that now can be answered

Analyze how much time is spent per ride:

1. Based on date and time factors such as day of week and time of day
2. Based on which station is the starting and / or ending station
3. Based on age of the rider at time of the ride
4. Based on whether the rider is a member or a casual rider

Analyze how much money is spent:

1. Per month, quarter, year
2. Per member, based on the age of the rider at account start

Analyze how much money is spent per member:

1. Based on how many rides the rider averages per month
2. Based on how many minutes the rider spends on a bike per month
