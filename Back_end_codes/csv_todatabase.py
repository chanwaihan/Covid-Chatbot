# import pyodbc
#
# DB server address
server = 'mysqlserver300.database.windows.net'
# connecting DB name
database = 'mySampleDatabase2'
# user name
username = 'cs15'
# user password
password = 'Computer@2020'

import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as sa

connection_uri = sa.engine.URL.create(
    "mssql+pyodbc",
    username='cs15',
    password='Computer@2020',
    host=server,
    database='mySampleDatabase2',
    query={"driver": "ODBC Driver 17 for SQL Server"},
)



df = pd.read_csv("C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Project/context_consumer.csv")
engine = create_engine(connection_uri, fast_executemany=True)
conn = engine.connect()

df.to_sql(name="CONTEXT", con=engine, if_exists='append', index=False)


