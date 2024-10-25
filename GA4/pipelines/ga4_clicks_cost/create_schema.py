import os
from GA4.sql.tables.ga4_clicks_cost import GA4_Clicks_Cost
from GA4.sql.engine import Engine
from dotenv import load_dotenv

load_dotenv()

# SQL Credentials
SQL_HOST=os.environ.get('host')
SQL_DB=os.environ.get('db')
SQL_USER=os.environ.get('user')
SQL_PSWD=os.environ.get('pass')


def main():
    sql_engine = Engine.mysql(
                                host=SQL_HOST,
                                db=SQL_DB,
                                user=SQL_USER,
                                pswd=SQL_PSWD
                                )

    create_table_job = GA4_Clicks_Cost(sql_engine)
    create_table_job.create_table()

    return None

if __name__ == '__main__':
    main()