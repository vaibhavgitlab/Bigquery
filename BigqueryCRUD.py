# module
from google.cloud.bigquery import dataset
import pandas as pd
from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/ADMIN/Documents/Work/ornate-destiny-333319-289b114aa0a6.json"

# Variables
project_name = 'ornate-destiny-333319'
dataset_name = 'Demo_bq'

# connections
client = bigquery.Client(project=project_name)
dataset_ref = client.dataset(dataset_name)

# Result to dataframe function

def Read():
    column = input("enter columns: ")
    if column != "*":
        query = client.query("SELECT {0} FROM `bigquery-public-data.austin_311.311_service_requests` LIMIT 1000".format(column))
        result = query.result()
        return result.to_dataframe()
    else:
        print("* is not allowed")

def Write():
    column = input("enter columns: ")
    values = input("enter values: ")
    if column != "*":
        query = client.query("INSERT INTO `bigquery-public-data.austin_311.311_service_requests` ({0}) VALUES ({1})".format(column, values))
        result = query.result()
        return result.to_dataframe()
    else:
        print("* is not allowed")

def Update():
    value= input("Enter col/values: ")
    condition = input("enter condition")
    if value != "*":
        query = client.query("UPDATE `bigquery-public-data.austin_311.311_service_requests` SET {0} WHERE {1};".format(value, condition))
        result = query.result()
        return result.to_dataframe()
    else:
        print("* is not allowed")

def Delete():
    condition = input("enter condition")
    if condition != "*":
        query = client.query("DELETE FROM `bigquery-public-data.austin_311.311_service_requests` WHERE {}".format(condition))
        result = query.result()
        return result.to_dataframe()
    else:
        print("* is not allowed")

# df = Read()
# df = Write()
# df = Update()
# df = Delete()
