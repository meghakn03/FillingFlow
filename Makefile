.PHONY: start up down test trigger retry failure backfill logs query
start:
	./scripts/start.sh
up: start
down:
	docker compose down
test:
	docker compose run --rm -v $(CURDIR)/tests:/opt/airflow/tests:ro dashboard python -m unittest discover -s tests -v
trigger:
	docker compose exec scheduler airflow dags trigger filingflow
retry:
	docker compose exec scheduler airflow dags trigger filingflow --conf '{"mode":"demo","simulate_retry":true}'
failure:
	docker compose exec scheduler airflow dags trigger filingflow --conf '{"mode":"demo","simulate_failure":true}'
backfill:
	docker compose exec scheduler airflow dags backfill filingflow -s 2025-01-01T06:00:00+00:00 -e 2025-01-03T06:00:00+00:00
logs:
	docker compose logs -f scheduler
query:
	docker compose exec dashboard python -c "from filingflow.lakehouse import query; print(query('SELECT mode, metric, count(*) FROM metrics GROUP BY ALL'))"
