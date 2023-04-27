from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, text
from sqlalchemy.exc import OperationalError
from os import environ
from time import sleep
from datetime import datetime
from math import *
from sys import exit
import numpy as np
from .config import *

def run():
    # TODO
    # to do all etl task together
    pass

# check if connection is ok
def connection_check(CS="MYSQL_CS", max_retry=1):
    for i in range(max_retry):
        try:
            engine = create_engine(environ[CS], pool_pre_ping=True, pool_size=10)
            return True, engine
        except OperationalError as e:
            sleep(0.1)
    return False, e


# Create destination table if not exists, based on table description
def table_existence(engine):
    MD_Obj = MetaData()
    # TO DO : "TBL_NAME/TBL_DESCRIPTION" should define as an argument
    DE_Task_Table =Table('DE_Task_Table', MD_Obj,
          Column('time', String(16), default='20230427 235959', primary_key=True),
          Column('device_id', String(64), default='None', primary_key=True),
          Column('total_points', Integer, default=0),
          Column('max_temperature', Integer, default=0),
          Column('total_distance', Float, default=0.0))
    try:
        MD_Obj.create_all(engine)
        return True, DE_Task_Table
    except Exception as e:
        return False, e

# get results of given database
def get_results(engine, query):
    try:
        with engine.connect() as conn:
            data = conn.execute(text(query))
            return True, data
    except Exception as e:
        return False, e


# insert into destination
def insert(engine, table, data):
    # data structure : (dev_id, max_temp, total_dist, currentTime)
    try:
        insert_obj = table.insert()
        with engine.connect() as conn:
            # walk through data
            for item in data:
                # Handel duplicate error
                try:
                    conn.execute(insert_obj, item)
                    conn.commit()
                    print(f'{item} was inserted in MySQL ...', flush=True)
                except Exception as e:
                    print(e)
        return True, None
    except Exception as e:
        return False, e
        

def get_distance(lat1, lon1, lat2, lon2):
    lat1 = float(lat1)
    lat2 = float(lat2)
    lon1 = float(lon1)
    lon2 = float(lon2)
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return 6371 * c

def transform(data):
    dataDic = {}
    res = []
    for datalog in data:
        if datalog[1] in dataDic.keys():
            dataDic[datalog[1]].append(datalog)
        else:
            dataDic[datalog[1]] = [datalog]

    currentTime = datetime.now().strftime('%x %X').replace('/','').replace(':','')
    
    # walk through data per device per hour
    for k, v in dataDic.items():
        # sort to keep order of seen location -> guarantee the correct distance
        v.sort()
        max_temp = int(v[0][2])
        total_dist = 0
        data_points = len(v)
        # Error Handling for devices with only one position
        if data_points > 1:
            for i in range(1, len(v)):
                if int(v[i][2]) > max_temp:
                    max_temp = int(v[i][2])
                total_dist += get_distance(v[i-1][3], v[i-1][4], v[i][3], v[i][4])
        hourlyDevRes = {'device_id': str(k), 'max_temperature': max_temp, 'total_points': data_points, 'total_distance': total_dist, 'time': currentTime}
        res.append(hourlyDevRes)
    return res

def save2csv(data):
    # TODO
    currTime = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    print(f'{OUTPUT_PATH}/output_{currTime}.csv')
    with open(f'{OUTPUT_PATH}/output_{currTime}.csv', 'w') as ofile:
        pass


