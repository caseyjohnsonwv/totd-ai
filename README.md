# TOTD-AI

Overengineering an entire data lake environment to support an AI application that predicts future Track Of The Day selections in Trackmania 2020, then making those predictions accessible via API routes that integrate with web browsers, Discord bots, and more.

<img src="./docs/architecture.png"/>

## Quickstart

Will require a beefy computer to run locally, 10GB+ of RAM.
1. Run `docker-compose up --build`
2. Navigate to the Airflow UI
3. Manually trigger the `master-ingestion-dag`

Airflow:
```
UI: localhost:8080
Username: airflow
Password: airflow
```

Adminer for Postgres:
```
UI: localhost:8081
System: PostgreSQL
Server: trackmania-postgres
Username: airflow
Password: airflow
Database: trackmania
```
