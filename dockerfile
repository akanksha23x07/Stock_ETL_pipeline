FROM apache/airflow:latest

USER root
RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean

# Set working directory
WORKDIR /opt/airflow

COPY requirements.txt requirements.txt
USER airflow
# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

USER root
# Copy project files into the container
COPY alphavantage_stock_etl.py /opt/airflow/
COPY etl_dag.py /opt/airflow/dags
COPY .env /opt/airflow/
COPY --chown=airflow:airflow .env /opt/airflow/

USER airflow