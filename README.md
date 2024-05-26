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

- I used Airbyte to simplify data ingestion, ensuring smooth integration between the source and the stage area, monitoring schema/column changes and ensuring the first full load and the remaining incremental ones using Airbyte's own resource, it's fantastic.
  ![image](https://github.com/LuisGustavoCorrea/End-To-End-Free-Tools-Data-Enginner/assets/18196788/a9d408fb-5665-49b7-b4f1-58a2def161e3)
## Data Quality
- We set clear expectations for our data and ensure its quality every step of the way.
  Here, every time the data is moved or transformed, a check is carried out to see if it is as expected according to the business rules.
  Data Quality is directly linked to how the business works too.

## Orchestration
- Efficiently and automatically orchestration all steps of the data workflow. From ingestion to transformation and delivery, Airflow robustly provided end-to-end data process management and monitoring.


![fluxo airflow Executado](https://github.com/LuisGustavoCorrea/End-To-End-Free-Tools-Data-Enginner/assets/18196788/71bb12ac-f925-4a3b-9789-6df1973efdbf)






Excited to share the successful completion of my latest Data Engineering project focusing on Data Quality Pipeline.

Here's a brief overview of the technologies used to extract, transform, load (ETL) and Data Quality :

Apache Airflow: Consegui orquestrar de forma eficiente e automatizada todas as etapas do fluxo de trabalho de dados. Desde a ingestão até a transformação e a entrega, o Airflow proporcionou de forma robusta o gerenciamento e monitoramento do processo de dados de ponta a ponta.


I was able to efficiently and automated orchestrate all steps of the data workflow. From ingestion to transformation and delivery, Airflow robustly provided end-to-end data process management and monitoring.

Airbyte: Utilizei o Airbyte para simplificar a ingestão de dados, garantindo uma integração suave entre a fonte e a stage area, monitorando mudanças de schemas/colunas e garantindo a primeira carga full e as demais incrementais usando o próprio recurso do Airbyte, que achei fantástico.

I used Airbyte to simplify data ingestion, ensuring smooth integration between the source and the stage area, monitoring schema/column changes and ensuring the first full load and the remaining incremental ones using Airbyte's own resource, it's fantastic.

Great Expectations: Estabelecemos as expectativas claras sobre nossos dados e garantimos sua qualidade em cada passo do caminho.
 Aqui todos vez que o dado é movimentado ou transformado é realizado um check se estão dentro do esperado de acordo com a regra do negócio.
 Data Quality está diretamente ligado de como o negócio funcionada também.

We set clear expectations for our data and ensure its quality every step of the way.
  Here, every time the data is moved or transformed, a check is carried out to see if it is as expected according to the business rules.
  Data Quality is directly linked to how the business works too.

 Metabase: para criar visualizações e insights acionáveis partir dos dados processados.
to create visualizations and actionable insights from the processed data.

 Docker: que nos proporcionou um ambiente de desenvolvimento consistente e escalável.

 which provided us with a consistent and scalable development environment.



