# Bikesharing ETL Pipeline

The Bikesharing ETL Pipeline project is a data engineering solution designed to automate the process of extracting, transforming, and loading (ETL) Capital Bikeshare trip data. The goal is to build a comprehensive data pipeline that handles large-scale data processing, enables real-time data analysis, and provides an interactive platform for data visualization.

This is a Dockerized, end-to-end data engineering project that ingests, processes, and visualizes Capital Bikeshare data. It features a scheduled ETL pipeline, real-time data flagging, partitioned storage in gads/output/, and interactive dashboards powered by Plotly.


## Project Goals

* Schedule weekly data ingestion and transformation jobs via Airflow
* Store cleaned datasets as partitioned Parquet files in MinIO
* Simulate real-time alerts for critical ride patterns
* Visualize insights with Metabase dashboards
* Deploy the entire stack using Docker Compose


## Tech Stack

| Component            | Tool/Technology                |
| -------------------- | ------------------------------ |
| Workflow Management  | Apache Airflow                 |
| Data Processing      | Pandas                         |
| Storage              | Local storage, Parquet         |
| Real-Time Simulation | Python generators              |
| Database             | PostgreSQL                     |
| Dashboarding         | Plotly                         |
| Containerization     | Docker, Docker Compose         |


## Project Structure

```
Docker1/
├──                  
│   ├── dags/
│   │   └── data
        └──heatmap
        └──output #where the partition lies
        └──bikeshare_etl.py

├── postgres/                                            
├── docker-compose.yml             
├── Dockerfile                    
├── .env                               
└── README.md                         

```

## Workflow
![Workflow](https://github.com/Data-Epic/Ayo-Mike-Bikeshare_ETL/blob/dev/Bikeshare_etl%20Diagram.svg)

## Setup Instructions

### 1. Clone the Repository

```bash
https://github.com/Data-Epic/Ayo-Mike-Bikeshare_ETL.git
```

### 2. Build and Launch Containers

```bash
docker build -t dev_apache_airflow:3.0.0
docker-compose up --build -d
```

### 3. Access the Interfaces

| Service    | URL                                            | Default Login                     |
| ---------- | ---------------------------------------------- | --------------------------------- |
| Airflow    | [http://localhost:8080](http://localhost:8080) | airflow / airflow                     |
| PostgreSQL | Host: `localhost:5432`                         | User: airflow / Password: airflow |


## Running the ETL Pipeline

1. Go to Airflow UI at `http://localhost:8080`
2. Trigger the DAG: `bikeshare_etl_pipeline`
3. Monitor each task and view logs


## Real-Time Flagging
* Simulates alerts for:
  * Rides over 45 minutes
  * Casual rides starting at midnight
* Implemented with a generator inside the DAG
* Alerts are printed in logs for tracking unusual patterns


## Data Visualization (Plotly)
![Workflow](https://github.com/Data-Epic/Ayo-Mike-Bikeshare_ETL/blob/dev/heatmap.png)
* Create visualizations

## Output Data
![Workflow](https://github.com/Data-Epic/Ayo-Mike-Bikeshare_ETL/blob/dev/directories.png)
* Cleaned and transformed `.parquet` files
* Partitioned by:
  * `member_casual`
  * `week_number`
* Stored inside `dags/output/` and in local sorage


### Airflow Dags
![Workflow](https://github.com/Data-Epic/Ayo-Mike-Bikeshare_ETL/blob/dev/stream_it.png)





