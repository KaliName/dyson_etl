from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import os

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

def do_this(**context):
    print(str(AIRFLOW_HOME))

dag = DAG("dyson_etl_dag", default_args=default_args, schedule_interval=None)


# Staging

stage_charactes = BashOperator(task_id="stage_charactes", bash_command="python3 /usr/local/airflow/src/extract/stage_load_characters.py", dag=dag)
stage_superheroes_power_matrix = BashOperator(task_id="stage_superheroes_power_matrix", bash_command="python3 /usr/local/airflow/src/extract/stage_load_superheroes_power_matrix.py", dag=dag)
stage_characters_stats = BashOperator(task_id="stage_characters_stats", bash_command="python3 /usr/local/airflow/src/extract/stage_load_characters_stats.py", dag=dag)

stage_characters_to_comics = BashOperator(task_id="stage_characters_to_comics", bash_command="python3 /usr/local/airflow/src/extract/stage_load_characters_to_comics.py", dag=dag)
stage_comics = BashOperator(task_id="stage_comics", bash_command="python3 /usr/local/airflow/src/extract/stage_load_comics.py", dag=dag)
stage_marvel_charactecters_info = BashOperator(task_id="stage_marvel_charactecters_info", bash_command="python3 /usr/local/airflow/src/extract/stage_load_marvel_characters_info.py", dag=dag)
stage_marvel_dc_characters = BashOperator(task_id="stage_marvel_dc_characters", bash_command="python3 /usr/local/airflow/src/extract/stage_load_marvel_dc_characters.py", dag=dag)

# Transformation
transform_character_data = BashOperator(task_id="transform_character_data", bash_command="python3 /usr/local/airflow/src/transform/transform_character_data.py", dag=dag)
transform_comic_data = BashOperator(task_id="transform_comic_stats", bash_command="python3 /usr/local/airflow/src/transform/transform_comic_data.py", dag=dag)
transform_join_character_data = BashOperator(task_id="final_character_join", bash_command="python3 /usr/local/airflow/src/transform/transform_join_character_data.py", dag=dag)

# Loading
load_transformed_character_data = BashOperator(task_id="load_transformed_character_data", bash_command="python3 /usr/local/airflow/src/load/load_transformed_character_data.py", dag=dag)

# Dag Seq
stage_charactes>>transform_join_character_data
stage_characters_stats>>transform_character_data
stage_superheroes_power_matrix>>transform_character_data

stage_characters_to_comics>>transform_comic_data
stage_comics>>transform_comic_data

transform_character_data>>transform_join_character_data

transform_join_character_data>>load_transformed_character_data

