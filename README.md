# Apple-Data-Analysis 

# Overview
This repository contains the ETL (Extract, Transform, Load) implementation for the apple data analysis. The solution is designed to handle diverse data formats and is implemented on Databricks using PySpark, Python, and Databricks utilities. The data model consists of dimensional data (Customers) and fact data (Transactions)


# Data Model
1.Dimensional Data: Customer data is stored as a Delta table on Databricks.
2.Fact Data: Transaction data is available in CSV format.

# ETL Solution Components

1. Reader_factory Notebook:
This notebook contains the Factory Method Design Pattern implementation for reading data from various sources. It provides a standardized interface to handle different data formats seamlessly. To read more about Factory Method Design, please visit https://www.geeksforgeeks.org/factory-method-for-designing-pattern/

2. Extractor Notebook:
The Extractor notebook includes the implementation code for extracting data from each source. It utilizes the Reader Factory to read data in the appropriate format.

3. Loader_factory Notebook:
The Loader notebook contains the actual loading logic for each data destination. It uses the Loader Factory to determine the correct loading procedure.

4. Transform Notebook:
This notebook is responsible for implementing various data transformations based on the specific requirements or business cases. It is the core component where data is shaped and prepared for analysis.

5. Apple_analysis Notebook:
The Apple_analysis  notebook outlines the steps to design and execute the ETL workflows for different requirements. It acts as a blueprint for the ETL process.


# Usage
To use this ETL solution, follow these steps:

1. Set Up Environment: Ensure that Databricks and all necessary libraries are set up and configured correctly.
2. Create Delta Table: First upload the customer.csv file in DBFS path and then click on New tab in the left side of databrick window and choose table option and then select
DBFS option and then choose filestore , you will see all the files which got uploaded so choose the required file and then choose the option to create table with UI. Once that is
done then choose the cluster that you have created and then you can see  the table definition ,you can rename the table and change the data type format required and then finally
select the table to create with UI option , finally your delta table will get create
3. Data Ingestion: Use the Reader_factory and Extractor notebooks to ingest data from the CSV files and other sources.
4. Data Transformation: Apply the required transformations using the Transform notebook to prepare the data for analysis.
5. Data Loading: Load the transformed data into the target system using the Loader_factory and Loader notebooks.
6. Workflow Execution: Use the Apple_analysis notebook to orchestrate and execute the ETL workflows.

# Prerequisite
1. Databricks environment
2. Pyspark
3. Python
4. Basic understanding about Factory Method Design Pattern
5. Basic undertanding about Delta table and CSV file handling

# Contribution
Contributions to this project are welcome. Please ensure that any contributions follow the existing design patterns and coding standards.
   





