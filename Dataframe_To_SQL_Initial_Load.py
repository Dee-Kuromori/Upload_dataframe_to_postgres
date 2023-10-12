import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

DB_USERNAME = 'consultants'
DB_PASSWORD = 'WelcomeItc@2022'
DB_HOST = 'ec2-3-9-191-104.eu-west-2.compute.amazonaws.com'
DB_PORT = '5432'
DB_NAME = 'testdb'

# Construct the connection string
DATABASE_URL = 'postgresql://consultants:%s@ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb' % quote_plus(
    "WelcomeItc@2022")

TABLE_NAME = 'fraudtable'

# Read the CSV file into a Pandas DataFrame
csv_file_path = r"C:\Users\Consultant\Documents\Assignments_training_bd\project\End_to_end_pipeline\LifeDatabaseAMAZON-master\End_To_End_Incremental_Load\Full_Load_Part_1\scripts\FraudAnalysisOneMillion.csv"
data = pd.read_csv(csv_file_path)
data["row_id"] = [x for x in range(1,len(data)+1)]

data_first_batch = data[data['row_id'] <= 10000]
# data_incremental = data[(data["row_id"] > 20000) & (data["row_id"] <= 30000)]

print(len(data))

# Create a SQLAlchemy engine and connect to the database
engine = create_engine(DATABASE_URL)

# # Upload the data to the database table
data_first_batch.to_sql(TABLE_NAME, engine, if_exists='append' ,index=False)

print('CSV data successfully uploaded to the database table.') 


