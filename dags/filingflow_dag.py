"""Small XComs reference durable artifacts shared by the local executor."""
import logging
import os
from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from filingflow import db, sec, lakehouse
from filingflow.alerts import failure_alert
from filingflow.pipeline import extract, read_artifact, validate_and_deduplicate, write_artifact


@dag(dag_id='filingflow', schedule='0 6 * * *', start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
     catchup=False, max_active_runs=1, tags=['sec', 'portfolio'],
     default_args={'retries': 2, 'retry_delay': timedelta(seconds=20),
                   'retry_exponential_backoff': True, 'execution_timeout': timedelta(minutes=20),
                   'on_failure_callback': failure_alert},
     description='Fetch → validate/dedupe → extract XBRL → PostgreSQL → publish')
def filingflow():
    @task
    def begin():
        ctx = get_current_context()
        conf = ctx['dag_run'].conf or {}
        start = ctx['data_interval_start'].date().isoformat()
        end = ctx['data_interval_end'].date().isoformat()
        mode = conf.get('mode', os.getenv('FILINGFLOW_MODE', 'demo'))
        if mode not in ('demo', 'live'):
            raise ValueError('mode must be demo or live')
        db.start_run(ctx['run_id'], mode, start, end)
        return dict(run_id=ctx['run_id'], start=start, end=end, mode=mode)

    @task
    def fetch_filings(run):
        ctx = get_current_context()
        conf = ctx['dag_run'].conf or {}
        if conf.get('simulate_failure'):
            raise RuntimeError('Intentional failure demonstration: retries will exhaust')
        if conf.get('simulate_retry') and ctx['ti'].try_number == 1:
            raise RuntimeError('Intentional transient failure: next attempt will recover')
        bundles = sec.fetch(os.getenv('FILINGFLOW_CIKS', '0000320193,0000789019').split(','),
                            run['start'], run['end'], run['mode'])
        logging.info('Fetched %d company bundles for [%s, %s)', len(bundles), run['start'], run['end'])
        path = write_artifact(run['run_id'], 'bronze', bundles)
        return path

    @task
    def validate_deduplicate(path):
        ctx = get_current_context()
        result = validate_and_deduplicate(read_artifact(path))
        write_artifact(ctx['run_id'], 'quarantine', result['rejected'])
        if result['rejected']:
            raise ValueError(f"Rejected {len(result['rejected'])} filings; inspect quarantine artifact")
        return write_artifact(ctx['run_id'], 'validated', result)

    @task
    def extract_metrics(path):
        return write_artifact(get_current_context()['run_id'], 'extracted', extract(read_artifact(path)))

    @task
    def publish_lakehouse(path, run):
        lakehouse.publish(run, read_artifact(path))
        return path

    @task
    def load_postgres(path, run):
        db.load(run['run_id'], run['mode'], read_artifact(path))
        return run['run_id']

    @task
    def publish_dashboard(pipeline_run_id):
        db.succeed(pipeline_run_id)
        logging.info('Published run %s to http://localhost:8501', pipeline_run_id)

    run = begin()
    extracted = extract_metrics(validate_deduplicate(fetch_filings(run)))
    publish_dashboard(load_postgres(publish_lakehouse(extracted, run), run))


filingflow()
