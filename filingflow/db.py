import os
from contextlib import contextmanager
import json

SCHEMA = '''
CREATE TABLE IF NOT EXISTS runs (
 run_id TEXT PRIMARY KEY, mode TEXT NOT NULL, interval_start TEXT NOT NULL,
 interval_end TEXT NOT NULL, status TEXT NOT NULL, started_at TIMESTAMPTZ DEFAULT now(),
 finished_at TIMESTAMPTZ, filings INTEGER DEFAULT 0, duplicates INTEGER DEFAULT 0,
 rejected INTEGER DEFAULT 0, error TEXT);
CREATE TABLE IF NOT EXISTS filings (
 mode TEXT NOT NULL, cik TEXT NOT NULL, accession TEXT NOT NULL, company TEXT NOT NULL,
 form TEXT NOT NULL, filed DATE NOT NULL, report_date DATE NOT NULL, document TEXT NOT NULL,
 missing_metrics JSONB NOT NULL, last_run_id TEXT REFERENCES runs(run_id),
 PRIMARY KEY(mode,cik,accession));
CREATE TABLE IF NOT EXISTS metrics (
 mode TEXT NOT NULL, cik TEXT NOT NULL, accession TEXT NOT NULL, metric TEXT NOT NULL,
 tag TEXT NOT NULL, value NUMERIC NOT NULL, unit TEXT NOT NULL, period_start TEXT NOT NULL,
 period_end DATE NOT NULL, PRIMARY KEY(mode,cik,accession,metric,period_start,period_end),
 FOREIGN KEY(mode,cik,accession) REFERENCES filings(mode,cik,accession));
CREATE TABLE IF NOT EXISTS alerts (
 id BIGSERIAL PRIMARY KEY, run_id TEXT NOT NULL, task_id TEXT NOT NULL,
 message TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT now(), delivered BOOLEAN DEFAULT false);
'''


@contextmanager
def connection():
    import psycopg2
    with psycopg2.connect(os.environ['DATABASE_URL'], connect_timeout=10) as conn:
        with conn.cursor() as cursor:
            cursor.execute(SCHEMA)
        yield conn


def start_run(run_id, mode, start, end):
    with connection() as conn, conn.cursor() as cur:
        cur.execute('''INSERT INTO runs(run_id,mode,interval_start,interval_end,status)
            VALUES (%s,%s,%s,%s,'running') ON CONFLICT(run_id) DO UPDATE
            SET status='running', error=NULL, finished_at=NULL''', (run_id, mode, start, end))


def load(run_id, mode, result):
    with connection() as conn, conn.cursor() as cur:
        for f in result['filings']:
            key = (mode, f['cik'], f['accessionNumber'])
            cur.execute('''INSERT INTO filings VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(mode,cik,accession) DO UPDATE SET company=EXCLUDED.company,
                missing_metrics=EXCLUDED.missing_metrics,last_run_id=EXCLUDED.last_run_id''',
                (*key, f['company'], f['form'], f['filingDate'], f['reportDate'],
                 f['primaryDocument'], json.dumps(f['missing_metrics']), run_id))
            cur.execute('DELETE FROM metrics WHERE mode=%s AND cik=%s AND accession=%s', key)
            for m in f['metrics']:
                cur.execute('INSERT INTO metrics VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                            (*key, m['metric'], m['tag'], m['value'], m['unit'],
                             m['period_start'], m['period_end']))
        cur.execute('''UPDATE runs SET filings=%s,duplicates=%s,rejected=%s WHERE run_id=%s''',
                    (len(result['filings']), result['duplicates'], len(result['rejected']), run_id))


def succeed(run_id):
    with connection() as conn, conn.cursor() as cur:
        cur.execute("UPDATE runs SET status='success',error=NULL,finished_at=now() WHERE run_id=%s", (run_id,))


def snapshot():
    from psycopg2.extras import RealDictCursor
    with connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        result = {}
        for name, query in {
            'runs': 'SELECT * FROM runs ORDER BY started_at DESC LIMIT 50',
            'filings': 'SELECT * FROM filings ORDER BY filed DESC LIMIT 100',
            'metrics': 'SELECT * FROM metrics ORDER BY period_end DESC LIMIT 300',
            'alerts': 'SELECT * FROM alerts ORDER BY created_at DESC LIMIT 30',
        }.items():
            cur.execute(query)
            result[name] = [dict(row) for row in cur.fetchall()]
        return result
