import os

# PATH
cwd = os.getcwd()
PROJECT_PATH = os.path.abspath(os.path.join(cwd, os.pardir))

# CONNECTION STRING
MYSQL_CS = "MYSQL_CS"

# PRE-DEFINED QUERIES
queries = {
    "TEST": "select * from devices;",
    "EXTRACT_QUERY": """SELECT * 
                      FROM devices 
                      WHERE time::bigint >= EXTRACT(EPOCH FROM (NOW() AT TIME ZONE 'UTC' - INTERVAL '1 hour'));""",
    "INSERT_QUERY": "INSERT INTO  analytics.DE_Table_Task (time ,device_id ,max_temperature ,data_points, total_distance) VALUES"
}
