import json
import logging
import os
import urllib.request
from .db import connection


def failure_alert(context):
    ti = context['task_instance']
    run_id = context['run_id']
    message = str(context.get('exception', 'Task failed'))[:2000]
    logging.error('FilingFlow failure: run=%s task=%s %s', run_id, ti.task_id, message)
    try:
        with connection() as conn, conn.cursor() as cur:
            cur.execute("UPDATE runs SET status='failed',error=%s,finished_at=now() WHERE run_id=%s",
                        (message, run_id))
            cur.execute('INSERT INTO alerts(run_id,task_id,message) VALUES (%s,%s,%s) RETURNING id',
                        (run_id, ti.task_id, message))
            alert_id = cur.fetchone()[0]
        webhook = os.getenv('ALERT_WEBHOOK_URL')
        if webhook:
            body = json.dumps({'text': f'FilingFlow failed: {run_id} / {ti.task_id}: {message}',
                               'run_id': run_id, 'task_id': ti.task_id}).encode()
            request = urllib.request.Request(webhook, data=body, headers={'Content-Type': 'application/json'})
            with urllib.request.urlopen(request, timeout=10):
                pass
            with connection() as conn, conn.cursor() as cur:
                cur.execute('UPDATE alerts SET delivered=true WHERE id=%s', (alert_id,))
    except Exception:
        logging.exception('Failure alert delivery failed; inspect Airflow task/scheduler logs')
