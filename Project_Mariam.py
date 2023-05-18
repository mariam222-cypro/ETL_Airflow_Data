from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import sys
sys.path.append('/opt/airflow/includes')
import queries
from airflow.decorators import task
from airflow.operators.python_operator import BranchPythonOperator
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime 
from airflow.operators.python_operator import PythonOperator
from airflow.operators import *
from airflow.providers import *
from airflow.providers.amazon.aws.transfers.sql_to_s3 import *
import emp_dim_insert_update 

from airflow.operators.dummy import DummyOperator
from airflow.decorators import task

@task.branch(task_id="branch_task")
def branch_func(ids_to_updatee):
    if ids_to_updatee == '':
        return ["dumm"]
    else  :
        return ["update_task_to_snowflake"]
    
    
with DAG(
    "Project_Mariam",
    start_date=datetime(2023,5,14), 
    catchup=False,
    schedule='@yearly'
    
) as dag:

    sql_to_s3_task_empsal = SqlToS3Operator(
        task_id="sql_to_s3_task_empsal",
        sql_conn_id='mariam_postgres_conn',
        aws_conn_id='mariam_aws_conn',
        query='SELECT * FROM finance.emp_sal',
        s3_bucket='staging.emp.data',
        s3_key='mariam_emp_sal.csv',
        replace=True    
    )
    sql_to_s3_task_empdetails = SqlToS3Operator(
        task_id="sql_to_s3_task_empdetails",
        sql_conn_id='mariam_postgres_conn',
        aws_conn_id='mariam_aws_conn',
        query='SELECT * FROM hr.emp_details',
        s3_bucket='staging.emp.data',
        s3_key='mariam_emp_details.csv',
        replace=True
    )

    join_and_detect_new_or_changed_rows = emp_dim_insert_update.join_and_detect_new_or_changed_rows()

    branch_op = branch_func("{{ ti.xcom_pull(task_ids='join_and_detect_new_or_changed_rows', key='ids_to_update') }}")

    update_task_to_snowflake = SnowflakeOperator(
        task_id='update_task_to_snowflake',
        sql=queries.UPDATE_DWH_EMP_DIM("{{ ti.xcom_pull(task_ids='join_and_detect_new_or_changed_rows', key='ids_to_update') }}"),
        snowflake_conn_id='mariam_snowfl_conn'
    )
    insert_task_to_snowflake = SnowflakeOperator(
        task_id="insert_task_to_snowflake",
        sql=queries.INSERT_INTO_DWH_EMP_DIM("{{ ti.xcom_pull(task_ids='join_and_detect_new_or_changed_rows', key='rows_to_insert') }}") ,
        snowflake_conn_id='mariam_snowfl_conn',
        trigger_rule='none_failed'
    )

    dummy=DummyOperator(task_id="dumm")



    [sql_to_s3_task_empsal,sql_to_s3_task_empdetails] >>join_and_detect_new_or_changed_rows>>branch_op>>[update_task_to_snowflake,dummy]>>insert_task_to_snowflake




