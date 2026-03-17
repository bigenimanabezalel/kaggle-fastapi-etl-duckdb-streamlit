
import polars as pl
import subprocess

from os import path, getcwd, listdir
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from uvicorn import run

# Create Application object

app = FastAPI()


# Location to the csv data files
csv_path = getcwd() + "\\data\\projects\\kaggle_fastapi_etl\\CSV"

csv_files = listdir(csv_path)

# Create a dictionary to store the dataframes created from the csv files into polars dataframe format

''' 
# split the file name and csv extension to get the name of the dataframe and read the csv file into polars dataframe format and store it in the dictionary with the name of the dataframe as key and the dataframe as value

'''
csv_frame_dict = {
    file.split(".")[0].lower(): pl.read_csv(source=f'{csv_path}\\{file}')
    for file in csv_files
}


"""
Helper Functions or Global Resources can be defined here
"""


def create_json_repr(key: str, sample_size: int = 10000, is_sampled: bool = False) -> list[dict]:
    """
    This function takes a key and a polars dataframe as input and returns a json representation of the dataframe with the key as the name of the dataframe and the value as the json representation of the dataframe
    """
    # Convert thr Polars DtaFrame into a JSON serializable format and return it as a response to the client
    df = csv_frame_dict[key].to_pandas()

    # Replace NaN with None (JSON-safe)
    df = df.fillna(value='', inplace=False)

    if is_sampled:
        df = df.sample(n=sample_size, random_state=42, replace=True)

    json_representation = [df.iloc[i].to_dict() for i in range(df.shape[0])]
    return json_representation


"""
Endpoints can be defined here
"""

# Create a root endpoint i.e It is a representational state used to return http response to the client to http request made to the server


@app.get(path='/home')
def home():
    return RedirectResponse(url='/docs')  # {"message": "Welcome to the home page of the FastAPI application"}

# Titanice dataset endpoint


@app.get(path='/titanic')
def get_titanic_data():
    return create_json_repr(key="titanic")


@app.get(path='/iris')
def get_iris_data():
    return create_json_repr(key="iris")


@app.get(path='/netflix_titles')
def get_netflix_titles_data():
    return create_json_repr(key="netflix_titles", sample_size=100, is_sampled=True)


@app.get(path='/world_happiness_report')
def get_world_happiness_data():
    return create_json_repr(key="world_happiness_report")

# Run the ETL script as a subprocess to populate the DuckDB database with the data from the API endpoints
subprocess.Popen(["python", "etl\\etl.py"])

"""
Server

"""
if __name__ == "__main__":

    run(app=app) # , host="127.0.0.1", port=8000
