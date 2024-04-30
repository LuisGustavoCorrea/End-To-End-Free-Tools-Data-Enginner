# Lab 5 - Job ETL

# Imports
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from great_expectations_provider.operators.great_expectations import (
   GreatExpectationsOperator,
)
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.empty import EmptyOperator
POSTGRES_STG_CONN_ID = "Postgre_stg_conn"
MY_POSTGRES_SCHEMA = "staging"
MY_GX_DATA_CONTEXT = "/usr/local/airflow/gx"


import pendulum

AIRBYTE_CONNECTION_ID = '296b4c62-4819-4fed-9abf-0169930a6694' 
# Argumentos
args = {'owner': 'airflow'}

# Argumentos default
default_args = {
    'owner': 'airflow',    
    #'start_date': airflow.utils.dates.days_ago(2),
    #'end_date': datetime(),
    #'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 5),
}

# Cria a DAG
#dag_airflow_airbyte = DAG(dag_id = "airbyte_example_airflow_dag",
#                   default_args = args,
#                   # schedule_interval='0 0 * * *',
#                   schedule_interval = '@once',  
#                   dagrun_timeout = timedelta(minutes = 60),
#                   description = 'Job ETL de Carga no DW com Airflow',
#                   start_date = airflow.utils.dates.days_ago(1)
#


group_DQ_dimensions=[]
group_update_dimensions=[]
sub_group_dim_cliente =[]
sub_group_dim_produto =[]
sub_group_dim_localidade=[]
sub_group_update_dim=[]

with DAG(dag_id='PROJECT1_GROUP_ELT_DATA_QUALITY_DW_POSTGRE',
        default_args={'owner': 'airflow'},
        schedule='@daily',
        start_date=pendulum.today('UTC').add(days=-1)
   ) as dag:

    #Extract Data from sources using Airbyte
   with TaskGroup(group_id='Extract_load_sources') as Extract_load_sources:
            #Task for connection to Airbyte job
            trigger_sync_source_destination = AirbyteTriggerSyncOperator(
              task_id="Airbyte_Sync_Fonte_Transacional",
              airbyte_conn_id='airbyte_conn',
              connection_id=AIRBYTE_CONNECTION_ID,
              asynchronous=True
        )
            #Wait sync task in Airbyte
            wait_for_sync_completion = AirbyteJobSensor(
            task_id='Airbyte_check_sync',
            airbyte_conn_id='airbyte_conn',
            airbyte_job_id=trigger_sync_source_destination.output
        )
            trigger_sync_source_destination >> wait_for_sync_completion 

    # Check Data Quality on dimensions
   with TaskGroup(group_id='Check_DQ_dimensions') as Check_DQ_dimensions: 
         
         with TaskGroup(group_id='Check_DQ_Source_dim_cliente') as Check_DQ_Source_dim_cliente:     
                            
                check_st_ft_cliente = GreatExpectationsOperator(
                    task_id="check_st_ft_cliente",
                    conn_id=POSTGRES_STG_CONN_ID,
                    data_context_root_dir=MY_GX_DATA_CONTEXT,
                    schema="staging",
                    data_asset_name="st_ft_clientes",
                    expectation_suite_name="ge_suite_st_ft_clientes",
                    return_json_dict=True
                    )
            
                check_st_ft_tipo_cliente = GreatExpectationsOperator(
                    task_id="check_st_ft_tipo_cliente",
                   conn_id=POSTGRES_STG_CONN_ID,
                    data_context_root_dir=MY_GX_DATA_CONTEXT,
                    schema="staging",
                    data_asset_name="st_ft_tipo_cliente",
                    expectation_suite_name="ge_suite_st_ft_tipo_cliente",
                    return_json_dict=True
                    )                 
                       
                [check_st_ft_cliente,check_st_ft_tipo_cliente]

                sub_group_dim_cliente.append(Check_DQ_Source_dim_cliente)

         with TaskGroup(group_id='Check_DQ_Source_dim_produto') as Check_DQ_Source_dim_produto:                 

            
                check_st_ft_produto= GreatExpectationsOperator(
                    task_id="check_st_ft_produto",
                    conn_id=POSTGRES_STG_CONN_ID,
                    data_context_root_dir=MY_GX_DATA_CONTEXT,
                    schema="staging",
                    data_asset_name="st_ft_produtos",
                    expectation_suite_name="ge_suite_st_ft_produtos",
                    return_json_dict=True
                    )
                    
                check_st_ft_categorias= GreatExpectationsOperator(
                    task_id="check_st_ft_categorias",
                    conn_id=POSTGRES_STG_CONN_ID,
                    data_context_root_dir=MY_GX_DATA_CONTEXT,
                    schema="staging",
                    data_asset_name="st_ft_categorias",
                    expectation_suite_name="ge_suite_st_ft_categorias",
                    return_json_dict=True
                    )
                
                check_st_ft_subcategorias= GreatExpectationsOperator(
                    task_id="check_st_ft_subcategorias",
                    conn_id=POSTGRES_STG_CONN_ID,
                    data_context_root_dir=MY_GX_DATA_CONTEXT,
                    schema="staging",
                    data_asset_name="st_ft_subcategorias",
                    expectation_suite_name="ge_suite_st_ft_subcategorias",
                    return_json_dict=True
                    )                
                      
                [check_st_ft_produto,check_st_ft_categorias,check_st_ft_subcategorias]
                sub_group_dim_produto.append(Check_DQ_Source_dim_produto)
  
   group_DQ_dimensions.append(Check_DQ_dimensions)
    
    #Update Data in Dimensions tables
   with TaskGroup(group_id='Update_Dimensions') as Update_Dimensions:                     
               
                    with TaskGroup(group_id='Update_dim_cliente') as Update_dim_cliente:
                            
                        sql_query_load_dim_cliente = """INSERT INTO dw.dim_cliente (id_cliente, nome, tipo)
                                                        SELECT  id_cliente, 
                                                                nome_cliente, 
                                                                nome_tipo
                                                        FROM staging.st_ft_clientes tb1, staging.st_ft_tipo_cliente tb2
                                                        WHERE tb2.id_tipo = tb1.id_tipo;"""
                    

                        sql_load_dim_cliente = PostgresOperator(sql = sql_query_load_dim_cliente,
                                                                task_id = "load_dim_cliente",
                                                                postgres_conn_id = POSTGRES_STG_CONN_ID)
                        
                    with TaskGroup(group_id='Update_dim_produto') as Update_dim_produto:

                      sql_query_load_dim_produto = """INSERT INTO dw.dim_produto (id_produto, nome_produto, categoria, subcategoria)
                                                SELECT id_produto, 
                                                    nome_produto, 
                                                    nome_categoria, 
                                                    nome_subcategoria
                                                FROM staging.st_ft_produtos tb1, staging.st_ft_subcategorias tb2, staging.st_ft_categorias tb3
                                                WHERE tb3.id_categoria = tb2.id_categoria
                                                AND tb2.id_subcategoria = tb1.id_subcategoria;"""
                    

                      sql_load_dim_produto = PostgresOperator(sql = sql_query_load_dim_produto,
                                                              task_id = "load_dim_produto",
                                
                                         postgres_conn_id = POSTGRES_STG_CONN_ID)
                        
                    [sql_load_dim_cliente,sql_load_dim_produto]                        
                    sub_group_update_dim.append(Update_Dimensions)

group_update_dimensions.append(Update_Dimensions)

check_st_ft_vendas = GreatExpectationsOperator(
        task_id="check_st_ft_vendas",
        conn_id=POSTGRES_STG_CONN_ID,
        data_context_root_dir=MY_GX_DATA_CONTEXT,
        schema="staging",
        data_asset_name="st_ft_vendas",
        expectation_suite_name="ge_suite_st_ft_vendas",
        return_json_dict=True
    )

#Truncate table Consolidado and Insert Incremental Data in Consolidado
sql_query_t = """TRUNCATE TABLE staging.st_consolidado;
                            insert into staging.st_consolidado (sk_produto,sk_cliente,sk_localidade,sk_tempo,quantidade,preco_venda,custo_produto,receita_vendas,resultado)
                            SELECT sk_produto,
                            sk_cliente,
                            sk_localidade,
                            sk_tempo, 
                            SUM(quantidade) AS quantidade, 
                            SUM(preco_venda) AS preco_venda, 
                            SUM(custo_produto) AS custo_produto, 
                            SUM(ROUND((CAST(quantidade AS numeric) * CAST(preco_venda AS numeric)), 2)) AS receita_vendas,
                            SUM(ROUND((CAST(quantidade AS numeric) * CAST(preco_venda AS numeric)), 2)) -SUM(ROUND((CAST(quantidade AS numeric) * CAST(custo_produto AS numeric)), 2)) AS resultado 
                        FROM staging.st_ft_vendas tb1, 
                            staging.st_ft_clientes tb2, 
                            staging.st_ft_localidades tb3, 
                            staging.st_ft_produtos tb4,
                            dw.dim_tempo tb5,
                            dw.dim_produto tb6,
                            dw.dim_localidade tb7,
                            dw.dim_cliente tb8
                        WHERE tb2.id_cliente = tb1.id_cliente
                        AND tb3.id_localidade = tb1.id_localizacao
                        AND tb4.id_produto = tb1.id_produto
                        AND to_char(tb1.data_transacao, 'YYYY-MM-DD') = to_char(tb5.data_completa, 'YYYY-MM-DD')
                        AND to_char(tb1.data_transacao, 'HH') = tb5.hora
                        AND tb2.id_cliente = tb8.id_cliente
                        AND tb3.id_localidade = tb7.id_localidade
                        AND tb4.id_produto = tb6.id_produto
                        GROUP BY sk_produto, sk_cliente, sk_localidade, sk_tempo;
                        """
transformation_staging = PostgresOperator(sql = sql_query_t,
                               task_id = "transformation_staging",
                               postgres_conn_id = POSTGRES_STG_CONN_ID
)

#Check Data Quality in Consolidado Table
check_st_consolidado = GreatExpectationsOperator(
        task_id="check_st_consolidado",
        conn_id=POSTGRES_STG_CONN_ID,
        data_context_root_dir=MY_GX_DATA_CONTEXT,
        schema="staging",
        data_asset_name="st_consolidado",
        expectation_suite_name="ge_suite_st_consolidado",
        return_json_dict=True
)

#Insert Incremental Data in Fact table
sql_query_load = """insert into dw.fato_vendas( sk_produto,
                                                      sk_cliente,
                                                      sk_localidade,
                                                      sk_tempo, 
                                                      quantidade, 
                                                      preco_venda, 
                                                      custo_produto, 
                                                      receita_vendas,
                                                      resultado)
                            SELECT
                                    sk_produto,
                                    sk_cliente,
                                    sk_localidade,
                                    sk_tempo, 
                                    quantidade, 
                                    preco_venda, 
                                    custo_produto, 
                                    receita_vendas,
                                    resultado
                            FROM staging.st_consolidado ;
                          """
load_dw_fato_vendas = PostgresOperator(sql = sql_query_load,
                               task_id = "load_dw_fato_vendas",
                               postgres_conn_id = POSTGRES_STG_CONN_ID
)

#Truncate all staging table for the next execution
sql_query_truncate_stg = """truncate table staging.st_ft_categorias;
                            truncate table staging.st_ft_cidades;
                            truncate table staging.st_ft_clientes;
                            truncate table staging.st_ft_produtos;
                            truncate table staging.st_ft_subcategorias;
                            truncate table staging.st_ft_tipo_cliente;
                            truncate table staging.st_ft_vendas;
                            truncate table staging.st_consolidado;
                          """
truncate_stage = PostgresOperator(sql = sql_query_truncate_stg,
                               task_id = "truncate_stage",
                               postgres_conn_id = POSTGRES_STG_CONN_ID
)

# Fluxo da DAG
Extract_load_sources >> group_DQ_dimensions[0] >>  check_st_ft_vendas >> transformation_staging >> group_update_dimensions[0] >> check_st_consolidado >> load_dw_fato_vendas >> truncate_stage