import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# 1) Connect to the database with SQLAlchemy
def connect():
    global engine
    try:
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        print("Starting the connection...")
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        engine.connect()
        print("Connected successfully!")
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

# 2) y 3) Create the table and Insert data
def run_sql_file(filepath):
    with open(filepath, 'r') as file:
        sql_script = file.read()
    with engine.connect() as connection:
        for statement in sql_script.split(';'):
            if statement.strip():
                connection.execute(text(statement))
        print(f"Executed {filepath}")

if __name__ == "__main__":
    engine = connect()
    
    if engine:
        run_sql_file('./src/sql/drop.sql')
        run_sql_file('./src/sql/create.sql')
        run_sql_file('./src/sql/insert.sql')

# 4) Use Pandas to read and display a table
df = pd.read_sql("SELECT * FROM publishers", con=engine)
print(df)