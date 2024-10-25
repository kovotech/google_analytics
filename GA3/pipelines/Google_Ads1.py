from GA3.modules.api import GA3Service
from GA3.config import SCOPES, KEY_FILE_LOCATION, VIEW_ID, Google_Ads1, sql_credentials
from GA3.modules.sql import Engine, Mapper, SQL
from dateutil import parser
import datetime as dt

STARTDATE="2023-01-01"
ENDDATE="2023-07-18"
DIMENSIONS=Google_Ads1['dimensions']
METRICS=Google_Ads1['metrics']

def main(startDate,endDate):
    startDate_ = parser.parse(startDate)
    endDate_ = parser.parse(endDate)
    date_ = startDate_
    while date_ <= endDate_:
        print(f"================{date_}=================",flush=True)
        print("Calling GA3 Api..",flush=True)
        api_job = GA3Service(
                            scopes=SCOPES,
                            key_file_location=KEY_FILE_LOCATION,
                            view_id=VIEW_ID
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
        df = Mapper.Google_Ads1(response)

        print("Importing data to SQL DB..",flush=True)
        SQL.Import(
                sql_engine=engine,
                tablename="GA3_Google_Ads1",
                df=df
                )
        
        print("Import Done.",flush=True)
        date_ += dt.timedelta(days=1)


if __name__ == '__main__':
    main(
        STARTDATE,ENDDATE
    )