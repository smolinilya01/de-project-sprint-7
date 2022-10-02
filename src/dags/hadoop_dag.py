import pendulum
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator


os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8' 


args = {
    "owner": "ilya",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

exec_date = '{{ ds }}'
data_raw_events_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/geo/events'
data_ods_events_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/ilyasmolin/data/geo/events'
user_zone_report_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/ilyasmolin/data/datamart/user_zone_report'
week_zone_report_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/ilyasmolin/data/datamart/week_zone_report'
recommendation_zone_report_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/ilyasmolin/data/datamart/recomendation_zone_report'


with DAG(
        'hadoop_dag',
        default_args=args,
        description='',
        catchup=False,
        schedule_interval='0 2 * * * ',
        start_date=pendulum.datetime(2022, 1, 2, tz="UTC"),
        tags=['pyspark', 'hadoop', 'hdfs', 'datalake', 'geo', 'datamart'],
        is_paused_upon_creation=True,
) as dag:
    start = DummyOperator(task_id='start')
    
    migrate_geo_events_task = BashOperator(
        task_id='migrate_geo_events',
        bash_command=f"spark-submit --master local /lessons/dags/scripts/migrate_geo_events.py {exec_date} {data_raw_events_path} {data_ods_events_path}",
        retries=3
    )

    built_user_zone_report_task = BashOperator(
        task_id='built_user_zone_report',
        bash_command=f"spark-submit --master local /lessons/dags/scripts/user_zone_report.py {data_ods_events_path} {user_zone_report_path}",
        retries=3
    )
    
    built_week_zone_report_task = BashOperator(
        task_id='built_week_zone_report',
        bash_command=f"spark-submit --master local /lessons/dags/scripts/week_zone_report.py {exec_date} {data_ods_events_path} {week_zone_report_path}",
        retries=3
    )

    built_recommendation_zone_report_task = BashOperator(
        task_id='built_recommendation_zone_report',
        bash_command=f"spark-submit --master local /lessons/dags/scripts/recommendation_zone_report.py {data_ods_events_path} {recommendation_zone_report_path}",
        retries=3
    )
    
    finish = DummyOperator(task_id='finish')
    
    (
        start 
        >> migrate_geo_events_task 
        >> [built_user_zone_report_task, built_week_zone_report_task, built_recommendation_zone_report_task] 
        >> finish
    )