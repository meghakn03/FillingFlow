import copy
import tempfile
import unittest
from unittest.mock import patch
from filingflow.sec import demo, rows, fetch
from filingflow.pipeline import validate_and_deduplicate, extract


class PipelineTests(unittest.TestCase):
    def test_dedup_and_metrics(self):
        result = extract(validate_and_deduplicate(demo('2025-01-02')))
        self.assertEqual(result['duplicates'], 1)
        self.assertEqual(len(result['filings']), 1)
        self.assertEqual(len(result['filings'][0]['metrics']), 3)
        self.assertEqual(result['filings'][0]['missing_metrics'], [])

    def test_quarantine(self):
        data = demo('2025-01-02')
        data[0]['filings'][0]['accessionNumber'] = 'bad'
        result = validate_and_deduplicate(data)
        self.assertEqual(len(result['rejected']), 1)
        self.assertEqual(len(result['filings']), 1)

    def test_periods_and_missing(self):
        data = demo('2025-01-02')
        facts = data[0]['facts']['facts']['us-gaap']
        quarter = facts['Revenues']['units']['USD'][0]
        ytd = dict(quarter, start='2024-01-01', val=200)
        facts['Revenues']['units']['USD'].extend([ytd, dict(quarter)])
        del facts['Assets']
        f = extract(validate_and_deduplicate(data))['filings'][0]
        self.assertEqual(len([m for m in f['metrics'] if m['metric'] == 'revenue']), 2)
        self.assertEqual(f['missing_metrics'], ['assets'])

    def test_conflicting_context_fails(self):
        data = demo('2025-01-02')
        facts = data[0]['facts']['facts']['us-gaap']['Assets']['units']['USD']
        facts.append(dict(facts[0], val=10))
        with self.assertRaises(ValueError):
            extract(validate_and_deduplicate(data))

    def test_historical_pages_and_half_open_interval(self):
        def columns(day, accession):
            return dict(accessionNumber=[accession], filingDate=[day], reportDate=[day],
                        form=['10-K'], primaryDocument=['annual.htm'])
        recent = columns('2025-01-03', '0000320193-25-000003')
        historical = columns('2025-01-01', '0000320193-25-000001')
        submissions = dict(name='Example', filings=dict(recent=recent, files=[
            dict(name='older.json', filingFrom='2024-01-01', filingTo='2025-01-02')]))
        with patch('filingflow.sec.get_json', side_effect=[submissions, historical, {}]) as get:
            result = fetch(['320193'], '2025-01-01', '2025-01-03', 'live')
        self.assertEqual(len(result[0]['filings']), 1)
        self.assertEqual(result[0]['filings'][0]['filingDate'], '2025-01-01')
        self.assertEqual(get.call_count, 3)

    def test_column_mismatch(self):
        with self.assertRaises(ValueError):
            rows(dict(accessionNumber=['x'], filingDate=[], reportDate=[], form=[], primaryDocument=[]))

    def test_lakehouse_idempotency(self):
        from filingflow import lakehouse
        result = extract(validate_and_deduplicate(demo('2025-01-02')))
        with tempfile.TemporaryDirectory() as tmp, patch.dict('os.environ', FILINGFLOW_DATA_DIR=tmp):
            run = dict(run_id='test', mode='demo')
            lakehouse.publish(run, result)
            lakehouse.publish(run, result)
            self.assertEqual(lakehouse.query('SELECT count(*) FROM metrics'), [(3,)])
            lakehouse.publish(dict(run_id='test2', mode='demo'), result)
            self.assertEqual(lakehouse.query('SELECT count(*) FROM filings'), [(1,)])
            changed = copy.deepcopy(result)
            changed['filings'][0]['metrics'].pop()
            lakehouse.publish(dict(run_id='test3', mode='demo'), changed)
            self.assertEqual(lakehouse.query('SELECT count(*) FROM metrics'), [(2,)])


if __name__ == '__main__':
    unittest.main()
