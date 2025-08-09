# Stock_ETL_pipeline
Automated ETL pipeline to extract stock price data and metadata from the Alpha Vantage API, transform it into a structured format, and load it into a PostgreSQL database hosted on AWS RDS. The pipeline is containerized with Docker and scheduled to run daily at 11:00 AM IST using Airflow DAG, ensuring up-to-date financial insights.
