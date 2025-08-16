FROM apache/airflow:2.9.0

# Install dependencies
RUN pip install markupsafe==2.0.1 \
    && pip install apache-airflow-providers-postgres \
    && pip install apache-airflow-providers-odbc \
    && pip install psycopg2-binary \
    && pip install plyvel \
    && pip install --no-cache-dir apache-airflow-providers-airbyte==4.0.0 \
    && pip install --no-cache-dir "apache-airflow-providers-airbyte[http]==4.0.0" \
    && pip install --upgrade cmake \
    && pip install --upgrade pyarrow==14.0.0 \
	&& pip uninstall dbt \
	&& pip install --no-cache-dir --upgrade dbt-core==1.9.0 dbt-postgres==1.9.0

USER root
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

USER airflow
RUN airflow db migrate