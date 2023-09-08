import contextlib
import hashlib
import json
from typing import Dict, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from vertica_connection import load_dataset_file_to_vertica

from airflow.decorators import dag

import pandas as pd
import pendulum
import vertica_python


@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def project_sprint6_load_group_log_to_staging():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    load_group_log = PythonOperator(
        task_id='load_group_log',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/group_log.csv',
            'schema': 'STV2023081256__STAGING',
            'table': 'group_log',
            'columns': ['group_id', 'user_id', 'user_id_from', 'event', 'datetime'],
            'type_override': {'user_id_from': 'Int64'}
            },
    )


    start >> [load_group_log] >> end


_ = project_sprint6_load_group_log_to_staging()
