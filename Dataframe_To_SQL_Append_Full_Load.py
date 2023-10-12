import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, Integer, String, func

DB_USERNAME = 'consultants'
DB_PASSWORD = 'WelcomeItc@2022'
DB_HOST = 'ec2-3-9-191-104.eu-west-2.compute.amazonaws.com'
DB_PORT = '5432'
DB_NAME = 'testdb'

# Construct the connection string
DATABASE_URL = 'postgresql://consultants:%s@ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb' % quote_plus(
    "WelcomeItc@2022")

TABLE_NAME = 'fraudtable'


# Create a SQLAlchemy engine and connect to the database
engine = create_engine(DATABASE_URL)

# To actually connect to the database, you need to use the connect method, like this:
connection = engine.connect()


# The SQLAlchemy ORM needs a SQLAlchemy session to interact with the database.
Session = sessionmaker(bind=engine)
session = Session()



# Read the CSV file into a Pandas DataFrame
csv_file_path = r"C:\Users\Consultant\Documents\Assignments_training_bd\project\End_to_end_pipeline\LifeDatabaseAMAZON-master\End_To_End_Incremental_Load\Full_Load_Part_1\scripts\FraudAnalysisOneMillion.csv"
data = pd.read_csv(csv_file_path)
data["row_id"] = [x for x in range(1,len(data)+1)]

sql = "select max(row_id) as max_row from "+ TABLE_NAME
df_postgres = pd.read_sql(sql,con=engine)

current_row = int(df_postgres.iloc[0])
increment = current_row + 10000

data_append = data[(data['row_id'] > current_row) & (data["row_id"] <= increment)]


print(len(data))

print(type(current_row))
print(current_row)




# Upload the data to the database table
data_append.to_sql(TABLE_NAME, engine, if_exists='append', index=False)

print('CSV data successfully uploaded to the database table.')


