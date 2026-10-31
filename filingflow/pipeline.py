import hashlib
import json
import logging
import math
import os
import re
from datetime import date
from pathlib import Path

log = logging.getLogger(__name__)
TAGS = {'revenue': ['RevenueFromContractWithCustomerExcludingAssessedTax', 'Revenues',
                    'SalesRevenueNet'], 'net_income': ['NetIncomeLoss'], 'assets': ['Assets']}


def validate_and_deduplicate(bundles):
    unique, rejected, seen = [], [], set()
    duplicates = 0
    for bundle in bundles:
        for filing in bundle['filings']:
            try:
                if not re.fullmatch(r'\d{10}-\d{2}-\d{6}', filing['accessionNumber']):
                    raise ValueError('invalid accession')
                if not re.fullmatch(r'\d{10}', bundle['cik']):
                    raise ValueError('invalid CIK')
                date.fromisoformat(filing['filingDate'])
                date.fromisoformat(filing['reportDate'])
                if filing['form'] not in ('10-K', '10-Q'):
                    raise ValueError('unsupported form')
                if not filing['primaryDocument']:
                    raise ValueError('missing primary document')
            except (ValueError, KeyError, TypeError) as exc:
                rejected.append({'filing': filing, 'reason': str(exc)})
                continue
            key = (bundle['cik'], filing['accessionNumber'])
            if key in seen:
                duplicates += 1
                continue
            seen.add(key)
            unique.append({'cik': bundle['cik'], 'company': bundle['company'],
                           **filing, 'facts': bundle['facts']})
    log.info('Validation: %d valid, %d duplicates, %d rejected', len(unique), duplicates, len(rejected))
    return {'filings': unique, 'duplicates': duplicates, 'rejected': rejected}


def extract(validated):
    output = []
    for filing in validated['filings']:
        metrics = []
        concepts = filing['facts'].get('facts', {}).get('us-gaap', {})
        for metric, tags in TAGS.items():
            for tag in tags:
                candidates = [f for f in concepts.get(tag, {}).get('units', {}).get('USD', [])
                              if f.get('accn') == filing['accessionNumber']
                              and f.get('end') == filing['reportDate']
                              and isinstance(f.get('val'), (int, float))
                              and math.isfinite(f['val'])]
                # Preserve all duration contexts (quarter and YTD), never silently conflate them.
                contexts = {}
                for fact in candidates:
                    key = (fact.get('start', ''), fact['end'])
                    if key in contexts and contexts[key]['val'] != fact['val']:
                        raise ValueError(f'Conflicting facts for {tag}: {key}')
                    contexts[key] = fact
                if contexts:
                    for (start, end), fact in sorted(contexts.items()):
                        metrics.append(dict(metric=metric, tag=tag, value=fact['val'],
                                            unit='USD', period_start=start, period_end=end))
                    break
        record = {k: v for k, v in filing.items() if k != 'facts'}
        record['metrics'] = metrics
        record['missing_metrics'] = sorted(set(TAGS) - {m['metric'] for m in metrics})
        output.append(record)
    return {**validated, 'filings': output}


def artifact_path(run_id, name):
    root = Path(os.getenv('FILINGFLOW_DATA_DIR', '/opt/airflow/data'))
    directory = root / hashlib.sha256(run_id.encode()).hexdigest()[:24]
    directory.mkdir(parents=True, exist_ok=True)
    return directory / (name + '.json')


def write_artifact(run_id, name, value):
    path = artifact_path(run_id, name)
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, sort_keys=True))
    temp.replace(path)
    log.info('Persisted artifact %s (%d bytes)', path, path.stat().st_size)
    return str(path)


def read_artifact(path):
    return json.loads(Path(path).read_text())
