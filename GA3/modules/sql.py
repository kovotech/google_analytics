import pandas as pd
import json
from dateutil import parser
from sqlalchemy import create_engine

class Engine:
    @staticmethod
    def mysql(host:str,db:str,user:str,pswd:str):
        engine = create_engine(f"mysql+pymysql://{user}:{pswd}@{host}/{db}")
        return engine

class Mapper:
    @staticmethod
    def All_Session_by_Channels(response):
        schema_dict = {
            "Date":[],
            "Default Channel Grouping":[],
            "Sessions":[],
            "Users":[]
        }

        data = response['reports'][0]['data']['rows']

        for row in data:
            schema_dict['Date'].append(parser.parse(row['dimensions'][0]))
            schema_dict['Default Channel Grouping'].append(row['dimensions'][1])
            schema_dict['Sessions'].append(row['metrics'][0]['values'][0])
            schema_dict['Users'].append(row['metrics'][0]['values'][1])

        df = pd.DataFrame(schema_dict)
        return df
    
    @staticmethod
    def Google_Ads1(response):
        schema_dict = {
            "Date":[],
            "Clicks":[],
            "Cost":[],
            "CPC":[],
            "CPM":[],
            "Impressions":[],
            "Sessions":[],
            "Users":[]
        }

        data = response['reports'][0]['data']['rows']

        for row in data:
            schema_dict['Date'].append(parser.parse(row['dimensions'][0]))
            schema_dict['Clicks'].append(row['metrics'][0]['values'][0])
            schema_dict['Cost'].append(row['metrics'][0]['values'][1])
            schema_dict['CPC'].append(row['metrics'][0]['values'][2])
            schema_dict['CPM'].append(row['metrics'][0]['values'][3])
            schema_dict['Impressions'].append(row['metrics'][0]['values'][4])
            schema_dict['Sessions'].append(row['metrics'][0]['values'][5])
            schema_dict['Users'].append(row['metrics'][0]['values'][6])

        df = pd.DataFrame(schema_dict)
        return df

    @staticmethod
    def Session_Social(response):
        schema_dict = {
            "Default Channel Grouping":[],
            "Date":[],
            "Source / Medium":[],
            "Sessions":[]
        }

        data = response['reports'][0]['data']['rows']

        for row in data:
            schema_dict['Default Channel Grouping'].append(row['dimensions'][0])
            schema_dict['Date'].append(parser.parse(row['dimensions'][1]))
            schema_dict['Source / Medium'].append(row['dimensions'][2])
            schema_dict['Sessions'].append(row['metrics'][0]['values'][0])

        df = pd.DataFrame(schema_dict)
        return df

class SQL:
    @staticmethod
    def Import(sql_engine:Engine,tablename:str,df:pd.DataFrame):
        df.to_sql(
                name=tablename,
                con=sql_engine,
                if_exists="append",
                index=False
                )