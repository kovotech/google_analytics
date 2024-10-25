from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, DateTime, BigInteger, DECIMAL, Integer, text, Boolean, Time, Text
import sqlalchemy
from sqlalchemy.schema import CreateTable
from datetime import datetime as dt
from dateutil import parser

class GA4_Clicks_Cost:
    def __init__(self,engine:sqlalchemy.engine.Engine) -> None:
        self.engine=engine

    @staticmethod
    def getTable() -> Table:
        TABLE = Table(
            "ga4_clicks_cost",MetaData(),
            Column('date',Date),
            Column('sessionCampaignName',Text),
            Column('advertiserAdClicks',BigInteger),
            Column('advertiserAdCost',DECIMAL(20,2))
        )
        return TABLE
    
    def create_table(self):
        table = GA4_Clicks_Cost.getTable()
        stmt = CreateTable(table,if_not_exists=True)
        with self.engine.begin() as connx:
            connx.execute(stmt)

    @staticmethod
    def map(src:dict):
        output_dict:dict = {}
        try:
            # date = parser.parse(src['date'])
            # date_str = dt.strftime(date,'%Y-%m-%d')
            output_dict['date'] = src['date']
        except:
            output_dict = None
        try:
            output_dict['sessionCampaignName'] = src['sessionCampaignName']
        except:
            output_dict = None
        try:
            output_dict['advertiserAdClicks'] = src['advertiserAdClicks']
        except:
            output_dict = None
        try:
            output_dict['advertiserAdCost'] = src['advertiserAdCost']
        except:
            output_dict = None
        
        return output_dict
    
    def insert_record(self,record:dict):
        payload = GA4_Clicks_Cost.map(record)
        table = GA4_Clicks_Cost.getTable()
        stmt = table.insert().values(payload)
        with self.engine.begin() as connx:
            connx.execute(stmt)