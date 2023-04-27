import os
import json
from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from bin.utils import *
from bin.config import *
from time import sleep
from sys import exit
import psycopg2
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, func, select, text

#####################################################

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
connect, mysql_engine = connection_check(MYSQL_CS, 10)
if not connect:
    print(f"MySQL Error : {mysql_engine}")
    exit()
else:
    print('Connection to MySQL is successful.')

#####################################################
# Destination table check
#####################################################
tblEx, table = table_existence(mysql_engine)
if not tblEx:
    print(f"Table Existence Error : {table}")
    exit()
else:
    print("Destination table exists.")

#####################################################
# loop
#####################################################
while True:
    #################################################
    # Extract data
    #################################################
    dataList = []
    state, data = get_results(psql_engine, queries["EXTRACT_QUERY"])
    if not state:
        print(f"Fetch Error : {data}")
        exit()
    else:
        print("Data has been fetched successfuly.")
        
        # Extract from cursur (eager avluation)
        for item in data:
            # its like [{"device_id": ???, ...}, ...]
            # 0->device_id, 1->temp, 2->{lat,long}, 3->time
            location = json.loads(item[2])
            # put time at first to sort easier
            itemT = (item[3], item[0], item[1], float(location['latitude']), float(location['longitude']))
            dataList.append(itemT)

    #################################################
    # Transform data
    #################################################
    try:
        finalResult = transform(dataList)
        print("Transform has done successfuly.")
    except Exception as e:
        print(f"Transform Error : {e}")
        exit()

    #################################################
    # Load data
    #################################################
    load_state, load_msg = insert(mysql_engine, table, finalResult)
    if not load_state:
        print(f"Load Error : {load_msg}")
        exit()
    else:
        print("Load has done successfuly.")


    #################################################
    # write to file and wait for an 1 hour
    #################################################
    print("Then next step will start 1 hour later from now!", flush=True)
    sleep(3600)
    #################################################

