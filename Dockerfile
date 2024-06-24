FROM apache/airflow
User root
RUN apt-get update && apt-get install -y \
    python3-dev postgresql postgresql-contrib python3-psycopg2 libpq-dev
User airflow
COPY requirements.txt /
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt