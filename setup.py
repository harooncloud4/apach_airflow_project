# -*- coding: utf-8 -*-
"""
Created on Sun Jun 23 17:44:26 2024

@author: Haroon
"""


#ms sql connection 
from src.tools.mssql_connection import  mssql_connection

#mysql connection 
from src.tools.mysql_connection import  mysql_connection

#create df 
from src.tools.sql_queries import create_df

#insert data into db table
from src.tools.insert_data_into_mysql import insert_data_into_mysql

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def fetch_and_insert_data():
    # mssql connection 
   engine,connection =  mssql_connection()
   
   #mysql connection
   mysql_engine,mysql_connection =  mysql_connection()
  
    
   #select data from mssql 
   df = create_df(connection)
    #insert data into mysql
   insert_data_into_mysql(mysql_connection,df)
   
   
   # # Check if connection is closed
   connection.close()
   if connection.closed:
         print("Connection is closed")
   else:
         print("Connection is open")
   
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'insert_data_hourly',
    default_args=default_args,
    description='A simple DAG to insert data every hour',
    schedule_interval=timedelta(hours=1),
)

t1 = PythonOperator(
    task_id='insert_data_task',
    python_callable=fetch_and_insert_data,
    dag=dag,
)
  