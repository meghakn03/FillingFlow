"""A local Parquet lake with atomic per-run snapshots; no cloud services required."""
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import duckdb


def publish(run, result):
    root = Path(os.getenv('FILINGFLOW_DATA_DIR', '/opt/airflow/data')) / 'lake'
    key = hashlib.sha256(run['run_id'].encode()).hexdigest()[:24]
    target = root / 'silver' / ('mode=' + run['mode']) / ('run=' + key)
    target.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as conn:
        conn.execute('''CREATE TABLE filings(cik VARCHAR, accession VARCHAR, company VARCHAR,
                        form VARCHAR, filed DATE, report_date DATE, run_id VARCHAR)''')
        conn.execute('''CREATE TABLE metrics(cik VARCHAR, accession VARCHAR, metric VARCHAR,
                        tag VARCHAR, value DECIMAL(38,4), unit VARCHAR, period_start VARCHAR,
                        period_end DATE, run_id VARCHAR)''')
        for f in result['filings']:
            conn.execute('INSERT INTO filings VALUES (?,?,?,?,?,?,?)',
                         [f['cik'], f['accessionNumber'], f['company'], f['form'],
                          f['filingDate'], f['reportDate'], run['run_id']])
            for m in f['metrics']:
                conn.execute('INSERT INTO metrics VALUES (?,?,?,?,?,?,?,?,?)',
                             [f['cik'], f['accessionNumber'], m['metric'], m['tag'], m['value'],
                              m['unit'], m['period_start'], m['period_end'], run['run_id']])
        for table in ('filings', 'metrics'):
            conn.execute(f'ALTER TABLE {table} ADD COLUMN published_at TIMESTAMPTZ')
            conn.execute(f'UPDATE {table} SET published_at=?', [datetime.now(timezone.utc)])
            path = target / (table + '.parquet')
            conn.table(table).write_parquet(str(path.with_suffix('.tmp')), compression='zstd')
            path.with_suffix('.tmp').replace(path)
        gold = root / 'gold' / ('mode=' + run['mode']) / ('run=' + key)
        gold.mkdir(parents=True, exist_ok=True)
        conn.execute('''CREATE TABLE summary AS SELECT ?::VARCHAR AS run_id,
                     ?::VARCHAR AS mode, count(*) AS filings, ?::INTEGER AS duplicates,
                     ?::INTEGER AS rejected FROM filings''',
                     [run['run_id'], run['mode'], result['duplicates'], len(result['rejected'])])
        path = gold / 'summary.parquet'
        conn.table('summary').write_parquet(str(path.with_suffix('.tmp')), compression='zstd')
        path.with_suffix('.tmp').replace(path)
    return str(target)


def query(sql):
    """Views deduplicate across run snapshots; latest run wins for each filing."""
    root = Path(os.getenv('FILINGFLOW_DATA_DIR', '/opt/airflow/data')) / 'lake'
    with duckdb.connect() as conn:
        for table in ('filings', 'metrics'):
            pattern = str(root / 'silver' / 'mode=*' / 'run=*' / (table + '.parquet'))
            conn.read_parquet(pattern, hive_partitioning=True).create_view('raw_' + table)
        conn.execute("""CREATE VIEW filings AS SELECT * EXCLUDE(rn) FROM
            (SELECT *, row_number() OVER(PARTITION BY mode,cik,accession
             ORDER BY published_at DESC,run_id DESC) rn FROM raw_filings) WHERE rn=1""")
        conn.execute("""CREATE VIEW metrics AS SELECT m.* FROM raw_metrics m JOIN filings f
            ON m.mode=f.mode AND m.cik=f.cik AND m.accession=f.accession AND m.run_id=f.run_id""")
        return conn.sql(sql).fetchall()
