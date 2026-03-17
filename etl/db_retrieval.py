from duckdb import connect
from os import getcwd




db_path = getcwd() + "\\data\\projects\\kaggle_fastapi_etl\\etl_results.db"

# Connect to the DuckDB database using context manager to ensure proper resource management (automatic closing of connection)

with connect(db_path) as cursor:
    # Executing a query to fetch the data from the database and store it in a dataframe format 

    query = """ SELECT DISTINCT table_name
            FROM information_schema.columns"""

    df = cursor.sql(query=query).df()

    df.info()
    print(df.head())





