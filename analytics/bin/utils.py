# Standard lib --> Third parties lib --> Local lib
from math import *
from sys import exit
from os import environ
from time import sleep
from datetime import datetime
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, text
from sqlalchemy.exc import OperationalError
import numpy as np
from .config import *


# check if connection is ok
def check_connection(connection_string="MYSQL_CS", max_retry=1):
    for i in range(max_retry):
        try:
            engine = create_engine(environ[connection_string], pool_pre_ping=True, pool_size=10)
            return True, engine
        except OperationalError as e:
            sleep(0.1)
    return False, e


# Create destination table if not exists, based on table description
def check_table_existence(engine):
    md_obj = MetaData()
    de_task_table = Table('de_task_table', md_obj,
                          Column('time', String(16), default='20230427 235959', primary_key=True),
                          Column('device_id', String(64), default='None', primary_key=True),
                          Column('total_points', Integer, default=0),
                          Column('max_temperature', Integer, default=0),
                          Column('total_distance', Float, default=0.0)
                          )
    try:
        md_obj.create_all(engine)
        return True, de_task_table
    except Exception as e:
        return False, e


# Get results of given database
def get_results(engine, query):
    try:
        with engine.connect() as conn:
            data = conn.execute(text(query))
            return True, data
    except Exception as e:
        return False, e


# Transform data to given form
def transform(data):
    data_dic = {}
    res = []
    for data_log in data:
        if data_log[1] in data_dic.keys():
            data_dic[data_log[1]].append(data_log)
        else:
            data_dic[data_log[1]] = [data_log]
    current_time = datetime.now()\
        .strftime('%x %X')\
        .replace('/', '')\
        .replace(':', '')
    # walk through data per device per hour
    for k, v in data_dic.items():
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
                total_dist += get_distance(v[i - 1][3], v[i - 1][4], v[i][3], v[i][4])
        hourly_device_data = {'device_id': str(k),
                              'max_temperature': max_temp,
                              'total_points': data_points,
                              'total_distance': total_dist,
                              'time': current_time
                              }
        res.append(hourly_device_data)
    return res


# insert into destination
def insert(engine, table, data):  # data structure : (dev_id, max_temp, total_dist, currentTime)
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


# Find distance between 2 points
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
