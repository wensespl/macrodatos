import pandas as pd
import os
from sqlalchemy import create_engine
import psycopg2

# Obtener el directorio actual de trabajo
current_directory = os.getcwd()

# Definir la ruta completa al archivo CSV
file_path = os.path.join(current_directory, 'linkedin_jobs_database.csv')

# Read the CSV file into a DataFrame
df = pd.read_csv(file_path)

# Database connection details
db_user = 'postgres'
db_password = '123456'
db_host = 'localhost'
db_port = '5432'
db_name = 'postgres'

# Create the SQLAlchemy engine
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Insert data into PostgreSQL
table_name = 'linkedin_jobs'  # You can name your table as desired
df.to_sql(table_name, engine, if_exists='replace', index=False)

print("Data inserted successfully")