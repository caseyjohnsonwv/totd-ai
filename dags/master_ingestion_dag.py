from datetime import datetime
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id = 'master_ingestion_dag',
    start_date = datetime(2023, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
    tags = ['master']
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')

    tmio_collection_layer = TriggerDagRunOperator(
        task_id = 'tmio_collection_layer',
        trigger_dag_id = 'collect_tmio_raw',
        wait_for_completion = True,
        poke_interval = 5
    )
    
    tmio_collection_layer_enhancements = TriggerDagRunOperator(
        task_id = 'tmio_collection_layer_enhancements',
        trigger_dag_id = 'collect_tmio_enhancements_raw',
        wait_for_completion = True,
        poke_interval = 5
    )

    tmio_conform_layer = TriggerDagRunOperator(
        task_id = 'tmio_conform_layer',
        trigger_dag_id = 'conform_tmio_cleaned',
        wait_for_completion = True,
        poke_interval = 5
    )

    tmio_conform_layer_enhancements = TriggerDagRunOperator(
        task_id = 'tmio_conform_layer_enhancements',
        trigger_dag_id = 'conform_tmio_enhancements_cleaned',
        wait_for_completion = True,
        poke_interval = 5
    )

    tmx_collection_layer = TriggerDagRunOperator(
        task_id = 'tmx_collection_layer',
        trigger_dag_id = 'collect_tmx_raw',
        wait_for_completion = True,
        poke_interval = 5
    )

    tmx_collection_layer_enhancements = TriggerDagRunOperator(
        task_id = 'tmx_collection_layer_enhancements',
        trigger_dag_id = 'collect_tmx_enhancements_raw',
        wait_for_completion = True,
        poke_interval = 5
    )

    tmx_conform_layer = TriggerDagRunOperator(
        task_id = 'tmx_conform_layer',
        trigger_dag_id = 'conform_tmx_cleaned',
        wait_for_completion = True,
        poke_interval = 5
    )

    tmx_conform_layer_enhancements = TriggerDagRunOperator(
        task_id = 'tmx_conform_layer_enhancements',
        trigger_dag_id = 'conform_tmx_enhancements_cleaned',
        wait_for_completion = True,
        poke_interval = 5
    )

    chain(start_task,
          tmio_collection_layer, tmio_conform_layer,
          tmx_collection_layer, tmx_conform_layer,
          [tmx_collection_layer_enhancements, tmio_collection_layer_enhancements],
          [tmx_conform_layer_enhancements, tmio_conform_layer_enhancements],
          end_task
    )
