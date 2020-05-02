Data Engineering Capstone

Introduction

The objective of this repository is to display the skills acquired in the Data Engineering Nanodegree from Udacity. Udacity provided datasets were used to design a data pipeline. These datasets included:
* U.S. Immigration data
* U.S. Demographics data
* Airport level data
* World Temperature data

A star schema was chosen to represent the above data . A star schema consists of fact tables surrounded by one or more dimension tables. 

* Fact tables: 
    * Immigration: This table contains records of persons entering the U.S. Arrival date, mode, visa type and personal information of the visitor is recorded here.
* Dimension tables:
    * Demogs: This table contains demographic information of U.S. states.
    * port: This table contains information of port of entry of the visitor.
    * country: This table contains information of the country associated with the visitor.
    * mode: This table contains information on the mode of transport used by the visitor.
    * visa: This table contains information on the type of visa issued to the visitor.
    * time: This table contains information of timestamps of records in immigration fact table broken down into specific units.

The above schema will allow users to analyze patterns in the behavior of immigrants entering the United States of America. Which countries are these people coming from? Are they disproportinately male or female? Is there is a preferred point of entry in the US for the immigrants? Last, but not the least is there any seasonality in the data and is immigration driven by the seasons of the country the immigrants are coming from?

Project Directory

The project directory contains the following files:

* create_tables.py: SQL queries for creating the data model tables
* I94_SAS_Labels_Descriptions.SAS: Label descriptions for the immigration data
* Capstone Project Template.ipynb: Notebook containing details around how the data pipeline was built, data dictionary and write-up of the project.
* etl.py: Script to run ETL process
* df.cfg: Config file for the project

Usage

The repository needs to be run in the following order:

* cd to home directory
* Configure the AWS credentials in the EC2 environment or edit the main() function in etl.py
* Edit the configuration file for necessary details
* Run the command: python etl.py