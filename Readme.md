# Airflow Docker Compose Setup
This repository contains the Docker Compose configuration necessary for setting up Apache Airflow with a CeleryExecutor, using PostgreSQL for the database and Redis as the broker. It is designed for development and testing purposes, not for a production environment.

![image](https://github.com/Sangmin-SHIM/AIRFLOW/assets/93679283/339affa3-2d5f-4885-b2c5-3fe32fd173ac)


## Flows and tasks
There are 2 chains of tasks in this project:
- The first chain consists of 3 tasks for downloading, cleaning and processing the `social data` in order to store it in the database.
- The second chain consists of 5 tasks for downloading, cleaning and processing the `elections` and `voting places coordinates data` in order to generate the html files of maps of elections data for all the voting places in France.



## Result
At the end of tasks Airflow, I could visualize the data with application POWER BI. Here are some example.

### 1) French President Election (2012, 2017, 2022)

- [Link](https://mspr.flusin.fr/html_files/12_T1.html)
- ![image](https://github.com/Sangmin-SHIM/AIRFLOW/assets/93679283/7d4827bd-f324-4a3e-abfc-d49751253fee)

### 2) DPAE (2000 ~ 2024)

- ![image](https://github.com/Sangmin-SHIM/AIRFLOW/assets/93679283/a9192aa8-595d-4385-ad8f-1f69fa23f03b)

### 3) Crime (20216 ~ 2023)

- ![image](https://github.com/Sangmin-SHIM/AIRFLOW/assets/93679283/a8189c3c-d3ae-48df-86bc-0a81a1b6a3de)

### 4) Death & Birth in France (2017)

- ![image](https://github.com/Sangmin-SHIM/AIRFLOW/assets/93679283/2c75b5ff-d76a-45ae-87e8-a6882da51468)

## Subject

For the subject that I've treated, you can see the details in `MSPR (French president election with social data).pdf`

![image](https://github.com/Sangmin-SHIM/AIRFLOW/assets/93679283/96739c4d-d229-4939-a1e5-21b87275fc1c)


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
- Import the necessary configuration variables by navigating to `Admin > Variables` in the Airflow dashboard. Here, use the Import Variables option to upload the `airflow_variable.json` file from your repository to configure the environment correctly.

![image](https://github.com/Sangmin-SHIM/AIRFLOW/assets/93679283/b9da42f4-f2cb-4d94-a99c-801f2acbcf42)

#### Accessing Airflow
The Airflow web interface can be accessed at http://localhost:8080 after the services are up and running. Use the credentials mentioned above to log in.

#### Important Notes
The Docker Compose file is configured with health checks for resilience and recovery of services.
The system's resource warnings (CPU, Memory, Disk) are logged if they fall below the required minimums for optimal operation.
For more detailed instructions, configurations, and best practices, please refer to the official Apache Airflow Documentation.
