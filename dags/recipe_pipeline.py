"""Three parallel ETL groups, failure isolation and a transactional quality gate."""
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from pipeline.core import SOURCES
from pipeline import tasks

with DAG(
    dag_id='international_recipe_pipeline',
    description='International recipes | APIs + respectful web scraping | PostgreSQL',
    start_date=pendulum.datetime(2026, 1, 1, tz='Asia/Bangkok'),
    schedule=None, catchup=False, max_active_runs=1,
    default_args={'owner': '6621600828', 'retries': 1, 'retry_delay': timedelta(seconds=15)},
    tags=['exam', 'recipes', 'postgresql'],
    doc_md='''### Recipe aggregation
Three independent TaskGroups execute extract → transform → load.
Source outages are recorded in PostgreSQL. Merge reads this run only.
Quality failure follows quarantine_and_stop and preserves the previous publication.
Use dag_run.conf: {"fail_sources":["mealdb"],"force_quality_failure":false}.
''',
) as dag:
    begin = PythonOperator(task_id='begin', python_callable=tasks.begin)
    merge = PythonOperator(task_id='merge_and_check', python_callable=tasks.merge_task, trigger_rule='all_done')
    for source in SOURCES:
        with TaskGroup(group_id=source) as group:
            extract = PythonOperator(task_id='extract', python_callable=tasks.extract_task, op_kwargs={'source': source})
            transform = PythonOperator(task_id='transform', python_callable=tasks.transform_task, op_kwargs={'source': source})
            load = PythonOperator(task_id='load', python_callable=tasks.load_task, op_kwargs={'source': source})
            extract >> transform >> load
        begin >> group >> merge
    branch = BranchPythonOperator(task_id='quality_branch', python_callable=tasks.choose_path)
    publish = PythonOperator(task_id='publish', python_callable=tasks.publish_task)
    quarantine = PythonOperator(task_id='quarantine_and_stop', python_callable=tasks.quarantine_task, retries=0)
    merge >> branch >> [publish, quarantine]
