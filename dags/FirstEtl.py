from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime
import requests
import psycopg2


dag = DAG(
        dag_id = 'first_etl',
        start_date = datetime(2021,5,19),
        schedule_interval = '0 12 * * *')


# Redshift connection 함수
def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "mjk3026"
    redshift_pass = "Mjk3026!1"
    port = 5439
    dbname = "prod"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()

def extract(url):
    f = requests.get(url)
    return (f.text)

def transform(text):
    lines = text.split("\n")
    return lines

def load(lines):
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    # BEGIN;DELETE FROM (본인의스키마).name_gender;INSERT INTO TABLE VALUES ('KEEYONG', 'MALE');....;END;
    cur = get_Redshift_connection()
    sql = "BEGIN;DELETE FROM mjk3026.name_gender;"
    for r in lines[1:]:
        if r != '':
            (name, gender) = r.split(",")
            print(name, "-", gender)
            sql += "INSERT INTO mjk3026.name_gender VALUES ('{n}', '{g}');".format(n=name, g=gender)
    sql += "END;"
    # print(sql)
    cur.execute(sql)

def execute_etl():
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    data = extract(link)
    lines = transform(data)
    load(lines) 


execute_etl = PythonOperator(
        task_id = 'execute_etl',
        #python_callable param points to the function you want to run
        python_callable = execute_etl,
        #dag param points to the DAG that this task is a part of
        dag = dag)
