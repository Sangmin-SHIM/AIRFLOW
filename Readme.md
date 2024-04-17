# Airflow Docker Compose Setup
This repository contains the Docker Compose configuration necessary for setting up Apache Airflow with a CeleryExecutor, using PostgreSQL for the database and Redis as the broker. It is designed for development and testing purposes, not for a production environment.


## Technical Details
This setup includes several services configured to ensure that Airflow runs smoothly:

- PostgreSQL: Used as the database for Airflow.
- Redis: Acts as the messaging broker for Celery.
- Airflow Webserver: Provides the web UI.
- Airflow Scheduler: Handles triggering scheduled tasks.
- Airflow Worker: Executes the tasks.
- Airflow Triggerer: Manages trigger-based workflows.
- Airflow CLI: Provides a command-line interface to interact with Airflow.
- Flower: A web-based tool for monitoring and administrating Celery clusters.
- Social PostgreSQL: An additional PostgreSQL service that is used for storing social data about the population of france.

## Prerequisites
Before starting, ensure Docker and Docker Compose are installed on your system. The configuration uses Docker Compose version 3.7 format.

## Setup Instructions
### Clone the Repository

```bash
git clone https://github.com/Sangmin-SHIM/AIRFLOW.git
cd AIRFLOW
git checkout MSPR_I1
```

### Start the Airflow Services

```bash
docker-compose up -d
```

This command builds the Airflow images if not already available and starts all the configured services in detached mode.

### Initial Setup and Airflow Variables
After starting the services, you need to make sure that Airflow is properly initialized:

- Visit the Airflow web interface at http://localhost:8080.
- Log in with the default credentials (username: airflow, password: airflow).
- Import the necessary configuration variables by navigating to Admin > Variables in the Airflow dashboard. Here, use the Import Variables option to upload the airflow_variables.json file from your repository to configure the environment correctly.
#### Accessing Airflow
The Airflow web interface can be accessed at http://localhost:8080 after the services are up and running. Use the credentials mentioned above to log in.

#### Important Notes
The Docker Compose file is configured with health checks for resilience and recovery of services.
The system's resource warnings (CPU, Memory, Disk) are logged if they fall below the required minimums for optimal operation.
For more detailed instructions, configurations, and best practices, please refer to the official Apache Airflow Documentation.