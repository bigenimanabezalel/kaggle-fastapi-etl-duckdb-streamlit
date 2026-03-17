import requests
import httpx
from fastapi import HTTPException

'''
requests only works in a synchronous context (WG), while httpx can be used in both synchronous and asynchronous contexts (AG). If you are working within an asynchronous framework like FastAPI, httpx is a better choice as it allows you to make non-blocking HTTP requests, which can improve the performance of your application.
'''
# GET request to the API endpoint to fetch the data from the csv files and return it as a json response to the client
api_url = "http://localhost:8000"

# End point to fetch the titanic dataset
titanic_endpoint = f"{api_url}/titanic"

# Attempting  a request 
response = requests.get(url=titanic_endpoint)
# JSON response from the API endpoint
data = response.json()

print(data)


# Using httpx to make a request to the API endpoint
response = httpx.get(url=titanic_endpoint)

data = response.json()

print(f"The data using httpx: {data}")



