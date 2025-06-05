# REST API for Platform Migration Benchmarking 

This project contains a REST API developed for **Invivo** to **test and benchmark** the performance of different data storage solutions during a platform migration. The focus is on evaluating speed, reliability, and AI-friendliness of various back-end architectures. 

>  Security across all tested platforms is implemented and managed by Invivo. 

## Project Objective 

Migrate and benchmark multiple data platforms to ensure optimal performance for AI-related workloads. The API allows automated performance testing across: 
- 🧱 MySQL (Local) 
- 🔷 Azure Serverless SQL 
- 🔷 Azure Synapse 
- 🔷 Databricks 

## Available APIs 

The application provides **5 REST endpoints**, each targeting a specific infrastructure or combination: 
1. **Local API** – Tests the performance of a local server with a MySQL database. 
2. **Databricks API** – Benchmarks operations on the Databricks platform. 
3. **Azure Serverless API** – Evaluates queries on an Azure Serverless SQL environment. 
4. **Azure Synapse API** – Measures query execution on Azure Synapse Analytics. 
5. **Global Benchmark API** – Simultaneously tests all three cloud platforms for comparative benchmarking. 

![image](https://github.com/user-attachments/assets/166532e7-1886-4086-8567-eef10262a07f)


--- 
## Tech Stack 
- **Backend**: Python, Flask 
- **Frontend**: Lightweight UI (custom-built for usability) 
- **Cloud Platforms**: Azure Serverless, Azure Synapse, Databricks 
- **Database**: MySQL (for local testing) 

--- 

## Frontend Preview 

![image](https://github.com/user-attachments/assets/5c7c7527-842a-47d0-8bc0-535ef2a76ac8)

![image](https://github.com/user-attachments/assets/63d85274-4307-43f0-a4d8-01948ad5209f)

![image](https://github.com/user-attachments/assets/32a167af-8d25-450e-80d8-453b3c494fbe)
