# Stock Data ETL Pipeline

## 📌 Overview
This project automates the extraction, transformation, and loading (ETL) of stock market data from the **Alpha Vantage API** into a **PostgreSQL** database hosted on **AWS RDS**.  
It retrieves both:
- Stock metadata (company details, market metrics, etc.)
- Daily stock prices (open, high, low, close, volume)

The pipeline is:
- **Containerized** with Docker
- **Orchestrated** with Apache Airflow
- **Scheduled** to run daily at **11:00 AM IST**

---

## ⚙️ Tech Stack
- **Python 3.11**
- **Alpha Vantage API**
- **PostgreSQL** (AWS RDS)
- **Apache Airflow**
- **Docker**
- **pandas**, **psycopg2**, **requests**, **python-dotenv**

---

## 🚀 Features
- Fetches real-time stock metadata and daily time-series data.
- Handles API errors and missing fields gracefully.
- Updates existing records only if data changes.
- Ignores duplicate entries in stock price history.
- Configurable stock symbols via CLI or default list.
- Daily automated execution using Airflow scheduler.

---

## 📂 Project Structure
.
├── airflow/dags/ # Airflow DAGs and ETL script
├── airflow/logs/dags # Airflow logs
├── scripts/ # ETL Python scripts
├── requirements.txt # Python dependencies
├── Dockerfile # Docker setup
├── docker-compose.yml # Airflow + Postgres services
├── extraction.log # Log file
└── README.md # Documentation

## 🔑 Environment Variables
Create a `.env` file in the project root with: 

ALPHA_VANTAGE_API_KEY=your_api_key
DB_NAME=your_db_name
DB_HOST=your_rds_endpoint
DB_USER=your_db_user
DB_PASS=your_db_password
DB_PORT=5432

🛠️ Setup & Run
1. Clone
git clone [https://github.com/akanksha23x07/Stock-ETL-pipeline.git](https://github.com/akanksha23x07/Stock_ETL_pipeline.git)
cd stock-etl-pipeline

2. Install Dependencies
pip install -r requirements.txt

3. Run with Docker and Airflow
docker-compose up -d
Airflow UI will be available at: http://localhost:8080
<img width="1675" height="493" alt="image" src="https://github.com/user-attachments/assets/5b042390-b5c2-45ea-9d94-cb3f7ad3b617" />


5. Run Locally
for default stock list --> python alphavantage_stock_etl.py
for specific company stock --> python alphavantage_stock_etl.py AAPL

## Database Schema
<img width="221" height="199" alt="image" src="https://github.com/user-attachments/assets/954e90c1-bf1a-4f00-a0ee-bd640f453d2f" />
<img width="316" height="670" alt="image" src="https://github.com/user-attachments/assets/47e2a2c3-04c0-40ce-8fd4-aba45c376386" />

## Sample DB screenshots
<img width="1675" height="389" alt="image" src="https://github.com/user-attachments/assets/5d7a181f-dfea-4caf-abaf-7f0af6c7da7b" />
<img width="1293" height="397" alt="image" src="https://github.com/user-attachments/assets/aa8577fd-fec1-4ae5-8bc7-237980a7bace" />





