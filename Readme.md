# Airflow Docker Compose Setup
This repository contains the Docker Compose configuration necessary for setting up Apache Airflow with a CeleryExecutor, using PostgreSQL for the database and Redis as the broker. It is designed for development and testing purposes, not for a production environment.
<img src="https://media.discordapp.net/attachments/1204411401821626458/1230150619264843887/image.png?ex=6632460a&is=661fd10a&hm=754feb388d2793e60ad66bcb94ecb58c80a9b0b1777c447ca55998001f1306c3&=&format=webp&quality=lossless">

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
<img src="https://media.discordapp.net/attachments/1204411401821626458/1230151255691759616/image.png?ex=663246a2&is=661fd1a2&hm=6d9c431c66a054f0fc6aed61148cdd2310f0be720df88c6a00341b5a143940d0&=&format=webp&quality=lossless">

#### Accessing Airflow
The Airflow web interface can be accessed at http://localhost:8080 after the services are up and running. Use the credentials mentioned above to log in.

#### Important Notes
The Docker Compose file is configured with health checks for resilience and recovery of services.
The system's resource warnings (CPU, Memory, Disk) are logged if they fall below the required minimums for optimal operation.
For more detailed instructions, configurations, and best practices, please refer to the official Apache Airflow Documentation.
