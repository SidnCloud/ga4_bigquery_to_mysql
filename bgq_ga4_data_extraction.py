# Importing needed modules
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime,timedelta
from sqlalchemy import create_engine
import concurrent.futures
import pandas as pd
import json

print("Loading extraction config from external file")
with open("./config.json","r") as f:
    config_file = json.loads(f.read())

print("Initializing Bigquery credentials and client")
credentials = service_account.Credentials.from_service_account_file('./credentials.json')
client = bigquery.Client(credentials=credentials, project=config_file['project_id'])

print("Elaborating sql queries for Bigquery data extraction")
property_id = config_file['property_id']
dataset = f"analytics_{property_id}"
SQLs = [f"SELECT * FROM {dataset}.{str((datetime.now()-timedelta(days=n)).date()).replace('-','')}" for n in [3,2,1]]

def get_bgq_results(sql:str) -> pd.DataFrame:
    print(f"Running query: {sql}")
    return client.query(sql).result().to_dataframe()


with concurrent.futures.ProcessPoolExecutor() as executor:
    futures_df = [future.result() for future in executor.map(get_bgq_results,SQLs)]

print("Concatenating results into a single dataFrame")
df = pd.concat(futures_df)

print("Initializing mysql environment to send ga4 data")
mysql_engine = create_engine(f"mysql+pymysql://{config_file['mysql_user']}:{config_file['mysql_pw']}@{config_file['mysql_host']}/{config_file['mysql_db']}")
mysql_table = config_file['mysql_table']

print(f"Sending data to {config_file['mysql_db']}")
df.to_sql(mysql_table, mysql_engine, index=False, if_exists='replace')

print("All done")