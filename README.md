# End-To-End-Free-Tools-Data-Enginner
![image](https://github.com/LuisGustavoCorrea/End-To-End-Free-Tools-Data-Enginner/assets/18196788/0b3333eb-838b-4843-a113-b05d87b62f90)

## Introduction
The goal of this project is to perform Data Analytics ensuring Data Quality throughout the entire Extraction, Transformation, and Load (ETL) process using various tools and technologies.

## Technology Used

- Programming Language - Python and SQL

1. Database - PostgreSQL
2. Ingestion - Airbyte
3. Data Quality - Great Expectations
4. Orchestration - Airflow
5. Data Visualization - MetaBase
6. Environment - Docker

## Data Model

 - Transactional Database (Source)
![image](https://github.com/LuisGustavoCorrea/End-To-End-Free-Tools-Data-Enginner/assets/18196788/3715fe70-4662-4f3f-854e-953be0d55fe5)
 - Data Warehouse ( Final Destination )
![image](https://github.com/LuisGustavoCorrea/End-To-End-Free-Tools-Data-Enginner/assets/18196788/c6cda9b3-6118-40b3-be26-a90f289535e5)

## Ingestion

- Airbyte: To simplify data ingestion, ensuring smooth integration between the source and the staging area, monitoring schema/column changes, and ensuring the first full load and the subsequent incremental ones using Airbyte's own resources, it's fantastic.
  ![image](https://github.com/LuisGustavoCorrea/End-To-End-Free-Tools-Data-Enginner/assets/18196788/a9d408fb-5665-49b7-b4f1-58a2def161e3)
## Data Quality
- We set clear expectations for our data and ensure its quality every step of the way.
  Here, every time the data is moved or transformed, a check is carried out to see if it is as expected according to the business rules.
  Data Quality is directly linked to how the business works too.

### Examples
- Checking null values
![image](https://github.com/LuisGustavoCorrea/End-To-End-Free-Tools-Data-Enginner/assets/18196788/2a4ee48f-ea18-4480-8681-747d317e107d)
- Checking if the values in the Quantidade column follow the business rules and expected data
![image](https://github.com/LuisGustavoCorrea/End-To-End-Free-Tools-Data-Enginner/assets/18196788/6a9a8f9f-c4a0-498c-9f16-e4b78a544bcd)
- Checking if the values in the Nome_tipo column follow the business rules and expected data
![image](https://github.com/LuisGustavoCorrea/End-To-End-Free-Tools-Data-Enginner/assets/18196788/bd293d84-19bb-4df4-9a01-1933dbe01f1b)

## Orchestration
- Efficiently and automatically orchestration all steps of the data workflow. From ingestion to transformation and delivery, Airflow robustly provided end-to-end data process management and monitoring.
![fluxo airflow Executado](https://github.com/LuisGustavoCorrea/End-To-End-Free-Tools-Data-Enginner/assets/18196788/71bb12ac-f925-4a3b-9789-6df1973efdbf)

## Data Visualization

- Create visualizations and actionable insights from the processed data.

![image](https://github.com/LuisGustavoCorrea/End-To-End-Free-Tools-Data-Enginner/assets/18196788/de53f8f3-f2a6-4992-88c8-d70b8ec418c9)
![image](https://github.com/LuisGustavoCorrea/End-To-End-Free-Tools-Data-Enginner/assets/18196788/e7baf20e-aa28-4be4-98be-840f923d28d3)

