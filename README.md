
# ETL_Airflow_Data
Data Pipeline for inserting/updating Annual Employee Salary scheduling using Airflow.

## Introduction

ETL_Airflow_Data is an Apache Airflow project designed to automate the extraction, transformation, and loading (ETL) process for synchronizing employee salary data. By leveraging the power of Apache Airflow, this project streamlines the flow of data from a PostgreSQL source system to a Snowflake data warehouse, ensuring accurate and up-to-date employee salary information.

## Project Overview

The main objective of this project is to enable seamless synchronization of employee salary data between the PostgreSQL source system and the Snowflake data warehouse. The project follows these key steps:

1. Extraction: Utilizing the PostgresOperator, the project extracts employee salary data from the PostgreSQL source system hosted on AWS. This step requires appropriate credentials and permissions to access the source system.
2. Staging: The extracted data is then staged in the Amazon S3 storage system using the S3UploadOperator. This operator securely uploads the data to a designated S3 bucket, preparing it for further processing.
3. Transformation: The project applies custom transformations to the extracted data using the powerful data manipulation capabilities of Pandas. The transformations are executed within the PythonOperator, allowing flexibility and customization in the transformation logic.
4. Update and Insert: Leveraging the SnowflakeOperator, the project updates existing employee salary records in the Snowflake data warehouse. The BranchPythonOperator is utilized to check if an employee record already exists in the data warehouse. If the record exists, it performs an update; otherwise, it inserts the new record.


## Prerequisites

Before running the project and executing the synchronization process, ensure that you have the following prerequisites in place:

- Access to the PostgreSQL source system hosted on AWS, along with valid credentials and appropriate permissions.
- An AWS account with access to Amazon S3 for staging data storage.
- Snowflake account credentials and access to the target data warehouse.
- A compatible operating system (Windows, Linux, macOS) with Python and the required dependencies installed.








