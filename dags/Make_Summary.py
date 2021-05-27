from airflow import DAG
from airflow.operators import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta
import requests
import logging
import psycopg2


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

def make_summary(**context):
    table = context["params"]["summary_table"]
    input_sql = context["params"]["summary_sql"]
    
    cur = get_Redshift_connection()
    sql = "BEGIN; DROP TABLE IF EXISTS {table};".format(table=table)
    sql += "CREATE TABLE {table} AS {input_sql} END;".format(table=table, input_sql=input_sql)

    logging.info(sql)
    cur.execute(sql)


dag_make_summary = DAG(
    dag_id = 'make_summary',
    start_date = datetime(2021,5,26), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval = '0 2 * * *',  # 적당히 조절
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

make_summary = PythonOperator(
    task_id = 'perform_make_summary',
    python_callable = make_summary,
    params = {
        'summary_table': Variable.get("summary_table"),
        'summary_sql': Variable.get("summary_sql")
    },
    provide_context=True,
    dag = dag_make_summary)

