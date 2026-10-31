FROM apache/airflow:2.10.5-python3.11
USER root
RUN mkdir -p /opt/airflow/data && chown airflow:root /opt/airflow/data
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
COPY filingflow /opt/airflow/filingflow
ENV PYTHONPATH=/opt/airflow
