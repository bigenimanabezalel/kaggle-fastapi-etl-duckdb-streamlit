import requests
import polars as pl


'''
- This script is used to test the API endpoints of the FastAPI application by making GET requests to the server and retrieving the data in a polars dataframe format and printing the first 5 rows of the dataframe to the console.


- Make sure to run the FastAPI application before running this script to test the API endpoints. You can run the FastAPI application by using the command "uvicorn app:app --reload" in the terminal. This will start the server and you can then run this script to test the API endpoints.'''


# url
url = "http://127.0.0.1:8000/netflix_titles"

# .get method to make a GET request to the server and store the response in a variable
response = requests.get(url=url)

# DataFrame to store the response in a polars dataframe format

df= None

# Let set the status code to 200 which means that the request was successful and the server returned a response to the client

if response.status_code == 200:
    df =pl.json_normalize(data=response.json())

    print(df.head())
    
