# Dockerized ETL Pipeline using Apache Airflow

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline using Apache Airflow.
The entire system is containerized using Docker Compose and uses PostgreSQL as the backend database.
Grafana is included for monitoring and visualization.

The goal of this project is to demonstrate real-world data engineering concepts such as workflow orchestration,
containerization, and modular ETL design.

## Technologies Used

- Apache Airflow – Workflow orchestration
- PostgreSQL – Metadata and target database
- Docker & Docker Compose – Containerized execution
- Grafana – Monitoring and dashboards
- Python – ETL logic and transformations

## Pipeline Flow

1. Airflow Scheduler triggers DAG execution
2. DAG tasks extract data from source files
3. Data transformations are applied using Python
4. Transformed data is loaded into PostgreSQL
5. System metrics and health can be monitored using Grafana


## How to Run the Project

### Prerequisites
- Docker
- Docker Compose

### Start Services
Run the following command from the project root directory:

docker-compose up -d
## Service Access

- Airflow Web UI: http://localhost:8081
- Grafana Dashboard: http://localhost:3000
- PostgreSQL: localhost:5432

## Testing

Testing is implemented using pytest.

Tests focus on:
- ETL logic validation
- Data correctness
- Regression testing for workflows

Run tests using:
pytest
## Dependency Management

All Python dependencies are listed in the requirements.txt file.

The runtime environment is controlled by Docker images to ensure consistency
across different machines.

## Best Practices Followed

- No secrets committed to the repository
- Fully containerized setup
- Modular and readable DAG design
- Version-controlled workflows
- Reproducible environment
  
## Author
Shamanth Kumar K, 

ETL & Data Engineering Internship Project
