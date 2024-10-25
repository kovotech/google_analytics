from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, DateTime, BigInteger, DECIMAL, Integer, text, Boolean, Time, Text
import sqlalchemy
from sqlalchemy.schema import CreateTable
from datetime import datetime as dt
from dateutil import parser

class GA4_Sessions:
    def __init__(self,engine:sqlalchemy.engine.Engine) -> None:
        self.engine=engine

    @staticmethod
    def getTable() -> Table:
        TABLE = Table(
            "ga4_sessions",MetaData(),
            Column('date',Date),
            Column('sessionSourceMedium',Text),
            Column('newUsers',BigInteger),
            Column('sessions',BigInteger)
        )
        return TABLE
    
    def create_table(self):
        table = GA4_Sessions.getTable()
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
            output_dict['sessionSourceMedium'] = src['sessionSourceMedium']
        except:
            output_dict = None
        try:
            output_dict['newUsers'] = src['newUsers']
        except:
            output_dict = None
        try:
            output_dict['sessions'] = src['sessions']
        except:
            output_dict = None
        
        return output_dict
    
    def insert_record(self,record:dict):
        payload = GA4_Sessions.map(record)
        table = GA4_Sessions.getTable()
        stmt = table.insert().values(payload)
        with self.engine.begin() as connx:
            connx.execute(stmt)