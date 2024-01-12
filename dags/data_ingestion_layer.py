
import os
import pandas as pd
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator




# /* DECLARE argument */

zone_raw : str = "raw_staging"
zone_pst : str = "pst_staging"
sourcePath      : str  = "/opt/airflow/data"
targetTable     : str  = "ProductSalesAmountByMonth"

connection : str = "postgres_default"
pg_hook_load = PostgresHook(postgres_conn_id=connection)


defaultArgs : dict = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'email': 'thanatthuch@msyne.co.th'
}


#################################################################################### Processing ####################################################################################
dag = DAG(
    'Data_Ingestion',
    default_args=defaultArgs,
    description='ingest data to table',
    schedule_interval=None
)

raw_truncate_command=f"TRUNCATE TABLE {zone_raw}.{targetTable};"
raw_truncate = PostgresOperator(
    task_id="raw_truncate",
    sql=raw_truncate_command,
    dag=dag
)

############################# RAW Process
def load_data_raw_zone():
    sql = f"""COPY {zone_raw}.{targetTable} (yearMonth, ProductId, ProductName, salesAmount, percentage_change)
        FROM stdin WITH CSV HEADER
        DELIMITER as ','
    """
    
    file_path = sourcePath + "/" +os.listdir(sourcePath)[0]
    print(pd.read_csv(file_path))
    pg_hook_load.copy_expert(sql=sql, filename=file_path)


raw_zone = PythonOperator(
    task_id = "raw_load",
    python_callable=load_data_raw_zone,
    dag=dag
)
    
############################# PST Process

pst_truncate_command = f"TRUNCATE TABLE {zone_pst}.{targetTable};"
pst_truncate = PostgresOperator(
    task_id= "pst_truncate",
    sql=pst_truncate_command,
    dag=dag
)

pst_zone_command=f"""INSERT INTO {zone_pst}.{targetTable} (ProductID, ProductName, salesAmount, percentage_change, start_date) SELECT cast(ProductID as int), ProductName ,cast(salesAmount as decimal(15,2)), COALESCE(cast(percentage_change as decimal(5,2)),0), TO_DATE(yearMonth || '-01', 'YYYY-MM-DD') as start_date  FROM {zone_raw}.{targetTable} 
"""
pst_zone = PostgresOperator(
    task_id="pst_load",
    sql=pst_zone_command,
    dag=dag
)



start = DummyOperator(
    task_id='start',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> raw_truncate >> raw_zone >> pst_truncate >> pst_zone >> end



#################################################################################### FILE Transformation Task1
# def file_transform():
#     data_files = os.listdir(sourcePath)
#     data_files.sort()

#     if not os.path.exists(sourceTransPath):
#         os.mkdir(sourceTransPath)

#     for file_name in data_files:
#         new_file_name = file_name.replace("parquet", "csv")
#         temp_df = pd.read_parquet(sourcePath +"/" +file_name)
#         temp_df.to_csv(sourceTransPath +"/"+ new_file_name, index=False)
#         print(f"----- ------ ------ -----`{new_file_name}` Transformed ----- ------ ------ -----")

# transformation = PythonOperator(
#     task_id = "parquet_to_csv",
#     python_callable=file_transform,
#     dag=dag
# )


# #################################################################################### TABLE Creation Task2
# createCommand=f"""CREATE TABLE IF NOT EXISTS {targetTable} (
#     department_name VARCHAR(32),
#     sensor_serial VARCHAR(64),
#     create_at TIMESTAMP,
#     product_name VARCHAR(16),
#     product_expire TIMESTAMP)"""

# create_table = PostgresOperator(
#     task_id="create_table",
#     sql=createCommand,
#     dag=dag
# )

# #################################################################################### TABLE Truncate Task3
# tuncateCommand=f"TRUNCATE TABLE {targetTable};"
# truncate_table = PostgresOperator(
#     task_id="truncate_table",
#     sql=tuncateCommand,
#     dag=dag
# )




# #################################################################################### Loading state Task4
# connection : str = "postgres_default"
# pg_hook_load = PostgresHook(postgres_conn_id=connection)

# def data_loading():
#     data_files = os.listdir(sourceTransPath)
#     data_files.sort()
#     total_ingest_file = 0

#     sql_command = f"""COPY {targetTable} (department_name, sensor_serial, create_at, product_name, product_expire)
#         FROM stdin WITH CSV HEADER
#         DELIMITER as ','
#     """

#     for file_name in data_files:

#         file_path = f"{sourceTransPath}/{file_name}"
#         # Open and execute the COPY command using the pg_hook

#         pg_hook_load.copy_expert(sql=sql_command, filename=file_path)
#         print(f"======================== `{file_name}` has loaded ========================")
#         total_ingest_file += 1

#     print(f"********************************** All files ingest SUCCEEDED  {total_ingest_file}**********************************")

# loading = PythonOperator(
#     task_id='load_csv_into_table',
#     python_callable=data_loading,
#     dag=dag,
# )


#################################################################################### PIPELINE ####################################################################################


# start >> transformation >> create_table >> truncate_table >> loading >> end