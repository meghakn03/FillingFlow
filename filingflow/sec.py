import json
import os
import time
import urllib.request
from datetime import date


def get_json(path):
    agent = os.environ.get('SEC_USER_AGENT', '')
    if '@' not in agent or 'example.com' in agent:
        raise ValueError('Set SEC_USER_AGENT to your name and real contact email')
    time.sleep(0.25)  # One serial fetch task; at most four requests/second.
    request = urllib.request.Request('https://data.sec.gov/' + path,
                                     headers={'User-Agent': agent, 'Accept-Encoding': 'identity'})
    with urllib.request.urlopen(request, timeout=45) as response:
        return json.load(response)


def rows(columns):
    keys = ('accessionNumber', 'filingDate', 'reportDate', 'form', 'primaryDocument')
    sizes = {len(columns[k]) for k in keys}
    if len(sizes) != 1:
        raise ValueError('SEC submission columns have mismatched lengths')
    return [dict(zip(keys, values)) for values in zip(*(columns[k] for k in keys))]


def fetch(ciks, start, end, mode):
    """Select filings in a half-open filing-date interval, including history pages."""
    if mode == 'demo':
        return demo(start)
    if mode != 'live':
        raise ValueError('mode must be demo or live')
    result = []
    for cik in ciks:
        cik = str(int(cik)).zfill(10)
        submissions = get_json(f'submissions/CIK{cik}.json')
        filings = rows(submissions['filings']['recent'])
        history = {}
        for page in submissions['filings'].get('files', []):
            if page['filingFrom'] < end and page['filingTo'] >= start:
                historical = get_json('submissions/' + page['name'])
                history[page['name']] = historical
                filings.extend(rows(historical))
        selected = [r for r in filings if start <= r['filingDate'] < end
                    and r['form'] in ('10-K', '10-Q')]
        if selected:
            facts = get_json(f'api/xbrl/companyfacts/CIK{cik}.json')
            result.append({'cik': cik, 'company': submissions['name'],
                           'filings': selected, 'facts': facts,
                           'source_submissions': submissions, 'source_history': history})
    return result


def demo(start):
    date.fromisoformat(start)
    accession = '0000320193-' + start[2:4] + '-' + start[5:].replace('-', '').zfill(6)
    filing = dict(accessionNumber=accession, filingDate=start, reportDate=start,
                  form='10-Q', primaryDocument='demo.htm')
    tags = {}
    for tag, value in [('Revenues', 85777000000), ('NetIncomeLoss', 21448000000),
                       ('Assets', 352583000000)]:
        fact = dict(accn=accession, end=start, val=value, form='10-Q', filed=start)
        if tag != 'Assets':
            fact['start'] = date.fromisoformat(start).replace(day=1).isoformat()
        tags[tag] = {'units': {'USD': [fact]}}
    return [{'cik': '0000320193', 'company': 'Apple Inc. (synthetic demo)',
             'filings': [filing, filing.copy()], 'facts': {'facts': {'us-gaap': tags}}}]
