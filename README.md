# Bees Tech Challenge

Overview
- Simple Airflow-based ETL that fetches brewery data and stores it in a local datalake.
- DAG lives in [dags/bees_dag.py](dags/bees_dag.py). The ingestion task is implemented by the function [`bees_dag.brewery_bronze`](dags/bees_dag.py).

Quick start
1. Build and run services with Docker Compose:
   ```sh
   docker compose up
   ```
   See [docker-compose.yaml](docker-compose.yaml).

2. Open the Airflow UI (default port 8080), login username and password is 'airflow' and enable the DAG bees_tech_challenge from [dags/bees_dag.py](dags/bees_dag.py).

Project layout
- [docker-compose.yaml](docker-compose.yaml) — container orchestration and environment variables.
- [dags/bees_dag.py](dags/bees_dag.py) — DAG definitions and ETL logic.
- datalake/
  - bronze/ — raw JSON pages produced by the DAG (examples: [datalake/bronze/brewery_page_1.json](datalake/bronze/brewery_page_1.json), ...).
  - silver/ — processed parquet partitions per country.

Data flow
1. The DAG calls the public brewery API and writes paginated JSON files into the bronze folder under the container path /home/airflow/datalake/bronze (mounted from the workspace `./datalake` via [docker-compose.yaml](docker-compose.yaml)).
2. Downstream tasks (noted in [dags/bees_dag.py](dags/bees_dag.py)) convert/partition data into the silver layer (Parquet files under [datalake/silver](datalake/silver)).
3. Gold layer shows the final view at the Airflow task log.

Important notes
- The DAG writes bronze files like `brewery_page_<n>.json` into the mounted datalake directory. Inspect [datalake/bronze](datalake/bronze) to confirm outputs.

Troubleshooting
- If DAG fails to write files, verify volume mounts in [docker-compose.yaml](docker-compose.yaml) and container permissions and restart Airflow container.
- Check scheduler and webserver logs under ./logs (mounted by compose).
- Check Airflow task logs to analyze about any error related to the ETL process.

Maintainers
- See the DAG implementation at [dags/bees_dag.py](dags/bees_dag.py) for code-level details.