# Importing needed modules
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from datetime import datetime,timedelta
from sqlalchemy import create_engine
import pandas as pd
import argparse
import json


class ga4_data_extraction:

    def __init__(self,config_file_path:str,cred_file_path:str) -> None:
        print("Loading extraction config from external file")
        with open(config_file_path,'r') as f:
            config_file = json.loads(f.read())
        
        print('Extracting config file data')
        self.project_id = config_file['project_id']
        self.property_id = config_file['property_id']
        self.dataset = f"analytics_{self.property_id}"

        print("Initializing Bigquery credentials and client")
        self.credentials = service_account.Credentials.from_service_account_file('./credentials.json')
        self.client = bigquery.Client(credentials=self.credentials, project=config_file['project_id'])

        print("Extracting MySQL config file data")
        self.mysql_user = config_file['mysql_user']
        self.mysql_pw = config_file['mysql_pw']
        self.mysql_host = config_file['mysql_host']
        self.mysql_db = config_file['mysql_db']
        self.mysql_table_events = config_file['mysql_table_events']
        self.mysql_table_users = config_file['mysql_table_users']

        print("Initialization done")

    def _elaborate_queries(self,daysAgo) -> list:
        data_addresses = [f"{self.dataset}.events_{str((datetime.now()-timedelta(days=n)).date()).replace('-','')}" for n in range(1,daysAgo+1)]
        return [f"SELECT * FROM  `{data_address}` WHERE platform!='WEB' AND event_name!='user_engagement'" for data_address in data_addresses]


    def _get_bgq_results(self,sql:str) -> pd.DataFrame:
        print(f"Running query: {sql}")
        try:
            return self.client.query(sql).result().to_dataframe()
        except NotFound as error:
            print(error)
        except:
            print(f'Unknown error for query: {sql}')
        return pd.DataFrame()

    def ga4_data_extraction(self,daysAgo) -> pd.DataFrame:
        
        print("Extracting data from BigQuery")
        df_list = []
        for sql in  self._elaborate_queries(daysAgo):
            df_list.append(self._get_bgq_results(sql))

        print("Concatenating results into a single dataFrame")
        df = pd.concat(df_list).reset_index(drop=True)

        return df

def main(daysAgo=3):
    data_extractor = ga4_data_extraction(config_file_path="./config.json",cred_file_path="./credentials.json")
    df = data_extractor.ga4_data_extraction(daysAgo)

    events_df = df.copy().drop('user_properties',axis=1)

    print('Fixing nested event_params column')
    eventParams_df = events_df['event_params'].explode().apply(lambda x: {x['key']:[value for _, value in x['value'].items() if value!=None][0]})
    eventParams_df = pd.DataFrame(pd.json_normalize(eventParams_df),index=eventParams_df.index)
    eventParams_df = eventParams_df.groupby(level=0).agg('first')
    for numCol in [col for col in eventParams_df.columns if pd.api.types.is_numeric_dtype(eventParams_df[col])]:
        eventParams_df[numCol] = eventParams_df[numCol].fillna(0)

    events_df = pd.concat([events_df.drop('event_params',axis=1),eventParams_df],axis=1)

    print('Fixing nested columns: app_info and device. Keeping app_version, operating_system and is_limited_ad_tracking')
    events_df['app_version'] = events_df['app_info'].apply(lambda x: x['version'])
    events_df = events_df.drop('app_info',axis=1)
    events_df = pd.concat([events_df.drop('device',axis=1), pd.json_normalize(events_df['device'])[['operating_system','is_limited_ad_tracking']]], axis=1)

    print('Fixing nested traffic_source column')
    events_df = pd.concat([events_df.drop('traffic_source',axis=1),pd.json_normalize(events_df['traffic_source'])],axis=1)
    events_df['channel_name'] = events_df['name']
    events_df = events_df.drop('name',axis=1)

    events_df = pd.concat([events_df.drop('geo',axis=1),pd.json_normalize(events_df['geo'])],axis=1)
    events_df['event_value_in_usd'] = events_df['event_value_in_usd'].fillna(0.0)
    events_df = pd.concat([events_df.drop('privacy_info',axis=1),pd.json_normalize(events_df['privacy_info'])],axis=1)

    events_df = events_df.drop(['user_ltv','previous_app_version','previous_os_version','update_with_analytics'],axis=1)

    events_df['p_key'] = events_df['user_id'] + "_" + events_df['event_name'] + "_" + events_df['event_timestamp'].astype(str)

    users_df = df.copy()[['user_id','user_pseudo_id','user_properties']].reset_index(drop=True)

    userProps_df = users_df['user_properties'].explode().apply(lambda x: {x['key']:[value for _, value in x['value'].items() if value!=None][0]})
    userProps_df = pd.DataFrame(pd.json_normalize(userProps_df),index=userProps_df.index)
    userProps_df = userProps_df.groupby(level=0).agg('last')
    userProps_df = userProps_df.drop('user_id',axis=1)

    users_df = pd.concat([users_df.drop('user_properties',axis=1),userProps_df],axis=1)
    users_df = users_df.groupby(['user_id','user_pseudo_id']).agg('last').reset_index()

    users_df = users_df.drop(users_df[users_df['user_id'].isin(['385','0','475'])].index)

    print("Initializing mysql environment to send ga4 data")
    mysql_engine = create_engine(f"mysql+pymysql://{data_extractor.mysql_user}:{data_extractor.mysql_pw}@{data_extractor.mysql_host}/{data_extractor.mysql_db}")
    mysql_table_events = data_extractor.mysql_table_events
    mysql_table_users = data_extractor.mysql_table_users

    print(f"Sending events data to {data_extractor.mysql_db} -> {mysql_table_events}")
    events_df.to_sql(mysql_table_events, mysql_engine, index=False, if_exists='replace')
    events_df['p_key'].value_counts()

    print(f"Sending users data to {data_extractor.mysql_db} -> {mysql_table_users}")
    users_df.to_sql(mysql_table_users, mysql_engine, index=False, if_exists='replace')

    print("All done")

if __name__=="__main__":
   parser = argparse.ArgumentParser(description="Extracts GA4 data from bigquery and send it to a mysql of choice")
   parser.add_argument('--days-ago',metavar='N',type=int,help="The number of days to extract, starting from yesterday. Default: 3.",default=3)
   args = parser.parse_args()

   main(args.days_ago)