"""CNPJ open data -> Bronze -> Silver -> Gold.

resolve_release -> release_pending? -> [per entity: download -> bronze -> silver]
    -> build_gold -> mark_release_done

Spark tasks share the ``spark`` pool (1 slot by default) so local runs never start
several JVMs competing for the same memory; downloads still run in parallel.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup

from cnpj_pipeline import tasks
from cnpj_pipeline.schemas import ENTITIES

SPARK_POOL = os.getenv("SPARK_POOL", "spark")
# Empty = manual runs only. With any schedule and catchup=False, Airflow starts the latest
# interval as soon as the DAG is unpaused, so `make run` would produce two runs.
SCHEDULE = os.getenv("CNPJ_SCHEDULE", "").strip() or None
RELEASE = "{{ ti.xcom_pull(task_ids='resolve_release') }}"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="cnpj_medallion",
    description="Receita Federal CNPJ open data through the Medallion architecture",
    start_date=datetime(2026, 1, 1),
    # Releases are monthly but on irregular days; in production CNPJ_SCHEDULE=@weekly plus
    # the release_pending gate picks up new releases within a week and is a no-op otherwise.
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "release": Param(
            "", type="string", description="Release date (YYYY-MM-DD). Empty = latest."
        ),
        "force": Param(
            False, type="boolean", description="Reprocess an already published release."
        ),
    },
    tags=["cnpj", "medallion", "spark", "delta"],
) as dag:
    resolve_release = PythonOperator(
        task_id="resolve_release",
        python_callable=tasks.resolve_release,
        op_kwargs={"requested": "{{ params.release or '' }}"},
    )

    release_pending = ShortCircuitOperator(
        task_id="release_pending",
        python_callable=tasks.is_release_pending,
        op_kwargs={"release": RELEASE, "force": "{{ params.force }}"},
    )

    build_gold = PythonOperator(
        task_id="build_gold",
        python_callable=tasks.build_gold,
        op_kwargs={"release": RELEASE},
        pool=SPARK_POOL,
        execution_timeout=timedelta(hours=6),
    )

    mark_done = PythonOperator(
        task_id="mark_release_done",
        python_callable=tasks.mark_release_done,
        op_kwargs={"release": RELEASE},
    )

    for entity in ENTITIES:
        with TaskGroup(group_id=entity) as entity_group:
            download = PythonOperator(
                task_id="download",
                python_callable=tasks.download_entity,
                op_kwargs={"entity": entity, "release": RELEASE},
                execution_timeout=timedelta(hours=3),
            )
            bronze = PythonOperator(
                task_id="load_bronze",
                python_callable=tasks.load_bronze,
                op_kwargs={"entity": entity, "release": RELEASE},
            )
            silver = PythonOperator(
                task_id="build_silver",
                python_callable=tasks.build_silver,
                op_kwargs={"entity": entity, "release": RELEASE},
                pool=SPARK_POOL,
                execution_timeout=timedelta(hours=4),
            )
            download >> bronze >> silver

        resolve_release >> release_pending >> entity_group >> build_gold

    build_gold >> mark_done
