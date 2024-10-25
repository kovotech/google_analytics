from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, DateTime, BigInteger, DECIMAL, Integer, text, Boolean, Time, Text
import sqlalchemy
from sqlalchemy.schema import CreateTable
from datetime import datetime as dt
from dateutil import parser

class GA4_User_Page_Visits:
    def __init__(self,engine:sqlalchemy.engine.Engine) -> None:
        self.engine=engine

    @staticmethod
    def getTable() -> Table:
        TABLE = Table(
            "ga4_user_page_visits",MetaData(),
            Column('date',Date),
            Column('unifiedPagePathScreen',Text),
            Column('totalUsers',BigInteger)
        )
        return TABLE
    
    def create_table(self):
        table = GA4_User_Page_Visits.getTable()
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
            output_dict['unifiedPagePathScreen'] = src['unifiedPagePathScreen']
        except:
            output_dict = None
        try:
            output_dict['totalUsers'] = src['totalUsers']
        except:
            output_dict = None
        
        return output_dict
    
    def insert_record(self,record:dict):
        payload = GA4_User_Page_Visits.map(record)
        table = GA4_User_Page_Visits.getTable()
        stmt = table.insert().values(payload)
        with self.engine.begin() as connx:
            connx.execute(stmt)