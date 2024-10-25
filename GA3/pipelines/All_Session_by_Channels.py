from GA3.modules.api import GA3Service
from GA3.config import SCOPES, KEY_FILE_LOCATION, VIEW_ID, All_Session_by_Channels, sql_credentials
from GA3.modules.sql import Engine, Mapper, SQL
from dateutil import parser
import datetime as dt

STARTDATE="2021-02-03"
ENDDATE="2023-04-30"
DIMENSIONS=All_Session_by_Channels['dimensions']
METRICS=All_Session_by_Channels['metrics']

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
        
        print("Initiating SQL Engine..",flush=True)
        engine = Engine.mysql(host=sql_credentials['host'],
                            db=sql_credentials['db'],
                            user=sql_credentials['user'],
                            pswd=sql_credentials['pass'])

        print("Mapping json response into dataframe..",flush=True)
        df = Mapper.All_Session_by_Channels(response)

        print("Importing data to SQL DB..",flush=True)
        SQL.Import(
                sql_engine=engine,
                tablename="GA3_All_Session_by_Channels",
                df=df
                )
        
        print("Import Done.",flush=True)
        date_ += dt.timedelta(days=1)


if __name__ == '__main__':
    main(
        STARTDATE,ENDDATE
    )