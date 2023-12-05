# Introductie
Campus Recruitment Inc. wants to build a dashboard based on internal data, enriched by 
open source data from Dienst Uitvoering Onderwijs (DUO). This codebase consists of an ETL 
application that will provide the data in Parquet format to the datawarehouse of Campus 
Recruitment. 

# Prerequisites
- Poetry package manager
- Python 3.11 or above

# Setup
To install the required Python packages, run: 
```shell
poetry install
```

For environment variables, copy the `.env.dist` file and replace the value with the actual
path to your Google credentials in the `.env` file.
```shell
cp .env.dist .env
```

# Usage
