REST API for Testing Platform Migration.
This repository contains a REST API developed as part of a project for Invivo. 
The API is designed to test and benchmark the performance of various solutions during the migration of a platform for data storage, with a focus on making the solution AI-friendly.

there is 5 API 
1) test on local server with MYSQL database
2) API for Databricks
3) API for Azure Serverless
4) API for Azure Synapse
5) global API that test the 3 connection at the same time

The API was developed in Python using the Flask framework and is designed to benchmark the performance of different platforms during the migration process. 
The platforms tested include:
Databricks
Azure Serverless
Azure Synapse

I implemented a small front so that it is more user friendly.


Note: Security is already implemented and managed by Invivo across the tested platforms.
