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

    tmio_collection_layer = TriggerDagRunOperator(
        task_id = 'tmio_collection_layer',
        trigger_dag_id = 'collect_tmio',
        wait_for_completion = True,
        poke_interval = 2
    )
    
    tmio_collection_layer_enhancements = TriggerDagRunOperator(
        task_id = 'tmio_collection_layer_enhancements',
        trigger_dag_id = 'collect_tmio_enhancements',
        wait_for_completion = True,
        poke_interval = 2
    )

    tmio_conform_layer = TriggerDagRunOperator(
        task_id = 'tmio_conform_layer',
        trigger_dag_id = 'conform_tmio',
        wait_for_completion = True,
        poke_interval = 2
    )

    tmio_conform_layer_enhancements = TriggerDagRunOperator(
        task_id = 'tmio_conform_layer_enhancements',
        trigger_dag_id = 'conform_tmio_enhancements',
        wait_for_completion = True,
        poke_interval = 2
    )

    tmx_collection_layer = TriggerDagRunOperator(
        task_id = 'tmx_collection_layer',
        trigger_dag_id = 'collect_tmx',
        wait_for_completion = True,
        poke_interval = 2
    )

    tmx_collection_layer_enhancements = TriggerDagRunOperator(
        task_id = 'tmx_collection_layer_enhancements',
        trigger_dag_id = 'collect_tmx_enhancements',
        wait_for_completion = True,
        poke_interval = 2
    )

    tmx_conform_layer = TriggerDagRunOperator(
        task_id = 'tmx_conform_layer',
        trigger_dag_id = 'conform_tmx',
        wait_for_completion = True,
        poke_interval = 2
    )

    tmx_conform_layer_enhancements = TriggerDagRunOperator(
        task_id = 'tmx_conform_layer_enhancements',
        trigger_dag_id = 'conform_tmx_enhancements',
        wait_for_completion = True,
        poke_interval = 2
    )

    totd_consume_layer = TriggerDagRunOperator(
        task_id = 'totd_consume_layer',
        trigger_dag_id = 'consume_totd',
        wait_for_completion = True,
        poke_interval = 2
    )

    totd_tags_consume_layer = TriggerDagRunOperator(
        task_id = 'totd_tags_consume_layer',
        trigger_dag_id = 'consume_totd_tags',
        wait_for_completion = True,
        poke_interval = 2
    )
    
    totd_authors_consume_layer = TriggerDagRunOperator(
        task_id = 'totd_authors_consume_layer',
        trigger_dag_id = 'consume_totd_authors',
        wait_for_completion = True,
        poke_interval = 2
    )

    chain(EmptyOperator(task_id = 'start_task'),
          tmio_collection_layer,
          tmio_conform_layer,
          tmx_collection_layer,
          tmx_conform_layer,
          [tmx_collection_layer_enhancements, tmio_collection_layer_enhancements],
          [tmx_conform_layer_enhancements, tmio_conform_layer_enhancements],
          EmptyOperator(task_id = 'ready_for_consume'),
          [totd_consume_layer, totd_tags_consume_layer, totd_authors_consume_layer],
          EmptyOperator(task_id = 'end_task'),
    )
