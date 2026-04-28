# Miami-Dade Animal Services Pipeline

A dockerized ETL pipeline and Streamlit dashboard for Miami-Dade Animal Services complaint data.

## Overview

This project fetches data from the Miami-Dade Animal Services ArcGIS API, cleans it using pandas, stores it in a PostgreSQL database, and visualizes it in a Streamlit dashboard. The pipeline is batch-oriented and only fetches new records on each run using ArcGIS ObjectId tracking.

<img width="668" height="585" alt="image" src="https://github.com/user-attachments/assets/22a2d6d4-d45b-4e55-bdf5-d343c15ca7d9" />


## Project Structure

```
├── extract.py          # Fetches data from the Miami-Dade ArcGIS API
├── transform.py        # Cleans and standardizes the data with pandas
├── load.py             # Orchestrates ETL and loads data into PostgreSQL
├── app.py              # Streamlit dashboard
├── Dockerfile          # Container image for ETL and Streamlit services
├── docker-compose.yml  # Runs postgres, ETL, and Streamlit in order
├── requirements.txt    # Python dependencies
├── .env.sample         # Environment variable template
└── README.md
```

## Setup

1. Copy `.env.sample` to `.env`:
   ```
   cp .env.sample .env
   ```

2. Build and run everything:
   ```
   docker-compose up --build
   ```

3. Open the dashboard at `http://localhost:8501`

## How It Works

`docker-compose up --build` runs three services in order:

1. **postgres** — starts the database
2. **etl** — runs `load.py`, which extracts from the API, transforms the data, and loads it into PostgreSQL
3. **streamlit** — starts after ETL completes and serves the dashboard

On the first run the ETL fetches all records. On subsequent runs it only fetches records with a higher ObjectId than what is already in the database.

To stop without losing data:
```
docker-compose down
```

## PostgreSQL Setup

The database schema is created automatically by `load.py` on first run using `CREATE TABLE IF NOT EXISTS`. No manual setup is required. Data is persisted in a Docker volume (`postgres_data`) between runs.

Database: `animal_db`, Host: `postgres` (internal), Port: `5432` (internal) / `5433` (host)

## Group Members

- Rivan Adhikari, Keshav Shrinivasan, Nick Morris, Kevin Eldho

