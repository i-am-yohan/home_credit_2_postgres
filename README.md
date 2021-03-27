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
unzip home-credit-default-risk.zip
```
This will result in the following CSVs being saved to the respective directory:
```
home-credit-default-risk.zip
application_test.csv
application_train.csv
HomeCredit_columns_description.csv
previous_application.csv
bureau_balance.csv
bureau.csv
sample_submission.csv
POS_CASH_balance.csv
installments_payments.csv
credit_card_balance.csv
```


## 2. Create Postgres Database
Using postgres sql command line, create postgres database as follows:
```
create database hm_crdt;
```
Ensure all privelages are granted:
```
GRANT ALL ON DATABASE hm_crdt TO <your_login_username>;
```

## 3. Run main ETL script
This is the main script that uploads the CSVs to a the `hm_crdt` DB. It is executed at follows:
```
python3 Hm_Crdt_2_Postgres.py <your_login_username> <password>
```
Ensure that this is run in the same directory in which the CSVs are saved. This can take a couple of hours to run.




