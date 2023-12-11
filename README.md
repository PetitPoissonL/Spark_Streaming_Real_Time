# Spark real-time project
## Introduction
___
This is a Spark Streaming real-time data processing project based on e-commerce data. 

The main data sources in the project are user behavior data obtained from logs and order data obtained from MySQL. 

The project ultimately presents data such as daily active users, orders, and user preferences in a visualized manner.

The project primarily utilizes tools such as Maxwell, Kafka, Spark, Redis, Elasticsearch, etc., for data processing, analysis, and visualization. Finally, it uses Spring Boot to create data interfaces.

## Prerequisites
___
Before setting up the project, ensure that you meet the following prerequisites:

- **Remote Servers**: The project requires three remote servers. These servers should be running on CentOS 7.5 operating system.

- **Server Naming**: The servers should be named as follows:
    - `hadoop102`
    - `hadoop103`
    - `hadoop104`

- **[Zookeeper Installation]()**: Zookeeper needs to be installed and properly configured for distributed coordination on each server.

- **[Kafka Installation]()**: Kafka should be installed on all three servers (`hadoop102`, `hadoop103`, `hadoop104`) to facilitate message queuing and streaming capabilities.

- **[Redis Installation]()**: Install and configure Redis on the `hadoop102` server for advanced key-value store and caching mechanisms.

- **[MySQL Installation]()**: MySQL is required for database management. Ensure it is installed and configured on the `hadoop102` server.

Ensure all these components are correctly installed and configured before proceeding with the setup of the project.

## Project Architecture
___
### 1. Overall Architecture

![Overall Architecture](https://github.com/PetitPoissonL/Project-Description-Image/blob/main/project_Spark_Streaming/Architecture_global.png)

### 2. Log data collection and Streaming

![Architecture ODS to DWD log](https://github.com/PetitPoissonL/Project-Description-Image/blob/e9ccb0cfab86788b0e09fa492d9a0f5539bf13f3/project_Spark_Streaming/ODS_to_DWD.png)

### 3. Business Data Collection and Streaming

![Architecture ODS to DWD DB](https://github.com/PetitPoissonL/Project-Description-Image/blob/main/project_Spark_Streaming/ODS_to_DWD_DB.png)


### 4. Data Processing from DWD to DWS Layer