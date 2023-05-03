# Standard lib -> Third parties lib -> Local lib 
from os import environ
from time import sleep
from sys import exit
import json
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import psycopg2
from bin.config import *
from bin.utils import *


print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL is successful.')

# My development
#####################################################
# Connection check
#####################################################
mysql_status, mysql_engine = check_connection(MYSQL_CS, 10)
if mysql_status is False:
    print(f"MySQL Error : {mysql_engine}")
    exit()
else:
    print('Connection to MySQL is successful.')

#####################################################
# Destination table check
#####################################################
table_status, table_obj = check_table_existence(mysql_engine)
if table_status is False:
    print(f"Table Existence Error : {table_obj}")
    exit()
else:
    print("Destination table exists.")


while True:
    #################################################
    # Extract data
    #################################################
    data_list = []
    fetch_status, fetch_cursor = get_results(psql_engine, queries["EXTRACT_QUERY"])
    if fetch_status is False:
        print(f"Fetch Error : {fetch_cursor}")
        exit()
    else:
        print("Data has been fetched successfully.")
        
        # Extract from cursor (eager evaluation)
        for item in fetch_cursor:  # its like [{"device_id": ???, ...}, ...]
            # 0->device_id, 1->temp, 2->{lat,long}, 3->time
            location = json.loads(item[2])
            # put time at first position to sort it easier
            shaped_item = (item[3], item[0], item[1], float(location['latitude']), float(location['longitude']))
            data_list.append(shaped_item)

    #################################################
    # Transform data
    #################################################
    try:
        transformed_data = transform(data_list)
        print("Transform has been done successfully.")
    except Exception as e:
        print(f"Transform Error : {e}")
        exit()

    #################################################
    # Load data
    #################################################
    load_status, load_msg = insert(mysql_engine, table_obj, transformed_data)
    if load_status is False:
        print(f"Load Error : {load_msg}")
        exit()
    else:
        print("Load has done successfully.")

    #################################################
    # write to file and wait for an 1 hour
    #################################################
    print("Then next step will start 1 hour later from now!", flush=True)
    sleep(3600)
