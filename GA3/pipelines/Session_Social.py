from GA3.modules.api import GA3Service
from GA3.config import SCOPES, KEY_FILE_LOCATION, VIEW_ID, Session_Social, sql_credentials
from GA3.modules.sql import Engine, Mapper, SQL
from dateutil import parser
import datetime as dt

STARTDATE="2021-09-20"
ENDDATE="2021-09-20"
DIMENSIONS=Session_Social['dimensions']
METRICS=Session_Social['metrics']

def main(startDate,endDate):
    startDate_ = parser.parse(startDate)
    endDate_ = parser.parse(endDate)
    date_ = startDate_
    while date_ <= endDate_:
        print(f"================{date_}=================",flush=True)
        print("Calling GA3 Api..",flush=True)
        api_job = GA3Service(
                            SCOPES,
                            KEY_FILE_LOCATION,
                            VIEW_ID
                            )
        date_str = date_.strftime('%Y-%m-%d')
        body = api_job.getBody(date_str,date_str,DIMENSIONS,METRICS)
        service = api_job.initialize_analyticsreporting()
        response = api_job.get_report(analytics=service,body=body)
        # print(response)
        print("Initiating SQL Engine..",flush=True)
        engine = Engine.mysql(host=sql_credentials['host'],
                            db=sql_credentials['db'],
                            user=sql_credentials['user'],
                            pswd=sql_credentials['pass'])
        
        print("Mapping json response into dataframe..",flush=True)
        df = Mapper.Session_Social(response)

        print("Importing data to SQL DB..",flush=True)
        SQL.Import(
                sql_engine=engine,
                tablename="GA3_Session_Social",
                df=df
                )
        
        print(f"{date_} data import done.",flush=True)

        date_ += dt.timedelta(days=1)

if __name__ == '__main__':
    main(
        STARTDATE,ENDDATE
    )