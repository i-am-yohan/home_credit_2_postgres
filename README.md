# Home Credit to Postgres ETL
A primative data pipeline that loads the Kaggle home credit dataset to a postgresql database.

Requirements:
 - Postgres 13
 - Kaggle API
 - Python 3

Python module dependencies:
 - psycopg2
 - csv
 - argparse

## 1. Download Home Credit Dataset from kaggle
This is done using the kaggle API. Details on how to use the kaggle API can be found at: https://github.com/Kaggle/kaggle-api

The command is as follows:
```
kaggle competitions download -c home-credit-default-risk
```
The file should be unzipped as follows:
```
unzip 
```
