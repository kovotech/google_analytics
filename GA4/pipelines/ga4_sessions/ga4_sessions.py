import os
import json
from dotenv import load_dotenv
from GA4.modules.api import ConstructGA4Object, GetGA4Data, ga4_response_to_list_of_dicts
from GA4.sql.engine import Engine
from GA4.sql.tables.ga4_sessions import GA4_Sessions
from GA4.modules.logger import myLogger, format_exception_logfile
from datetime import datetime as dt
import datetime
from dateutil import parser

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.environ.get('credential_file_location_server')

# SQL Credentials
SQL_HOST=os.environ.get('host')
SQL_DB=os.environ.get('db')
SQL_USER=os.environ.get('user')
SQL_PSWD=os.environ.get('pass')

# GA4 Credentials
PROPERTY_ID=os.environ['GA4_PROPERTY_ID']
DIMENSIONS=['date','sessionSourceMedium']
METRICS=['newUsers','sessions']

previous_date = dt.today()-datetime.timedelta(days=2)
previous_date_str = dt.strftime(previous_date,'%Y-%m-%d')
START_DATE=previous_date_str
END_DATE=previous_date_str

def main():
    #==================================== Getting Data from GA4 API ====================================
    print('Job Started...',flush=True)
    constuct_obj = ConstructGA4Object()
    dimensions_obj = constuct_obj.dimensions(DIMENSIONS)
    metrics_obj = constuct_obj.metrics(METRICS)
    print(f'Dimensions: {DIMENSIONS}',flush=True)
    print(f'Metrics: {METRICS}',flush=True)
    print(f'Start Date: {START_DATE}',flush=True)
    print(f'End Date: {END_DATE}',flush=True)

    print('Calling GA4 API...',flush=True)
    start_date_ = parser.parse(START_DATE)
    end_date_ = parser.parse(END_DATE)
    GA4_response_list = []
    while start_date_ <= end_date_:
        print("===============================================",flush=True)
        start_date_str = dt.strftime(start_date_,'%Y-%m-%d')
        print(f"Calling GA4 data for date: {start_date_str}")
        response = GetGA4Data.getData(propertyId=PROPERTY_ID,
                                    dimensions=dimensions_obj,
                                    metrics=metrics_obj,
                                    start_date=start_date_str,
                                    end_date=start_date_str)

        print('Serializing GA4 API response to python dictionary...',flush=True)
        temp_data = ga4_response_to_list_of_dicts(response,DIMENSIONS,METRICS)
        for record in temp_data:
            GA4_response_list.append(record)
        start_date_ += datetime.timedelta(days=1)
    #==================================== Getting Data from GA4 API ====================================
    #==================================== Importing Data to SQL ====================================
    print('Initiating SQL Engine...',flush=True)
    sql_engine = Engine.mysql(
                            host=SQL_HOST,
                            db=SQL_DB,
                            user=SQL_USER,
                            pswd=SQL_PSWD
                            )
    sql_obj = GA4_Sessions(engine=sql_engine)
    print('SQL Import Job Started...',flush=True)
    import_count = 1
    for record in GA4_response_list:
        sql_obj.insert_record(record)
        print(f'Records Imported {import_count}',end='\r',flush=True)
        import_count += 1
    #==================================== Importing Data to SQL ====================================
    print(f"Total Records Imported: {import_count-1}")
    return import_count-1

if __name__ == '__main__':
    try:
        imported_records_count = main()
        myLogger(level='info',filename='logs.log',msg=f'{imported_records_count} imported to ga4_sessions table')
    except Exception as e:
        formatted_log = format_exception_logfile(e)
        myLogger(level='error',filename='logs.log',msg=formatted_log)
    

    # data = main()
    # with open('test.json','w') as f:
    #     json.dump(data,fp=f,indent=3)