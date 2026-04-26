# 🚀 Real-Time Stocks Data Pipeline

![Python](https://img.shields.io/badge/Python-3776AB?logo=python\&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?logo=apacheairflow\&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Kafka-231F20?logo=apachekafka\&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker\&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake\&logoColor=white)


---

## 📌 Overview

This project builds a **real-time data pipeline** using the **Modern Data Stack**.
It ingests live stock market data, processes it in real time, and delivers analytics-ready insights.

---

## ⚡ Tech Stack

* **Python** → Data ingestion
* **Apache Kafka** → Real-time streaming
* **Apache Airflow** → Workflow orchestration
* **Snowflake** → Data warehouse
* **DBT** → Data transformation
* **Docker** → Containerization


---

## 🔥 Features

* 📡 Real-time stock data ingestion
* ⚡ Streaming pipeline using Kafka
* 🔄 Automated workflows with Airflow
* 🧱 Data modeling (Bronze → Silver → Gold)


---

## 📂 Project Structure

```
real-time-stocks-mds/
│
├── infra/              
├── logs/               
├── venv/               
├── requirements.txt
└── README.md
```

---

## 🚀 Getting Started

### 1. Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/real-time-stocks-mds.git
cd real-time-stocks-mds
```

### 2. Create virtual environment

```bash
python -m venv venv
```

### 3. Activate it (Windows)

```bash
venv\Scripts\activate
```

### 4. Install dependencies

```bash
pip install -r requirements.txt
```

---

## ⚙️ Pipeline Flow

1. Fetch stock data from API
2. Stream data using Kafka
3. Process data with Airflow
4. Store in Snowflake
5. Transform using DBT
6. Visualize in Power BI

---

## 📊 Output

* Cleaned datasets
* Analytical tables
* Real-time dashboards

---

## 👨‍💻 Author

**Hazem (Data Engineer)**

---

## ⭐ Notes

* `venv/` and `logs/` are excluded using `.gitignore`
* Make sure to configure API keys before running
