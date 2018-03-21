import os
import psycopg2

import pandas as pd
import numpy as np

os.chdir(r'C:\users\barby\downloads')

#%%
def dbGetQuery(q):
    pw = pd.read_table(r'c:\users\barby\documents\config.txt', header = None)[0].item().split('=')[1]
    conn_string = "host='kavdb.c9lrodma91yx.us-west-2.rds.amazonaws.com' dbname='kavdb' user='lkavenagh' password='" + pw + "'"
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    dat = pd.read_sql(q, conn)
    conn.close()
    return(dat)
    
def dbSendQuery(q):
    pw = pd.read_table(r'c:\users\barby\documents\config.txt', header = None)[0].item().split('=')[1]
    conn_string = "host='kavdb.c9lrodma91yx.us-west-2.rds.amazonaws.com' dbname='kavdb' user='lkavenagh' password='" + pw + "'"
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(q)
    cursor.close()
    conn.close()
    
def uploadDF(dat, table):
    q = 'INSERT INTO ' + table + ' (' + ",".join(up.columns) + ") VALUES ('"
    q = q + "'),('".join(up.astype(str).apply("','".join, axis=1))
    q = q + "')"
    pw = pd.read_table(r'c:\users\barby\documents\config.txt', header = None)[0].item().split('=')[1]
    conn_string = "host='kavdb.c9lrodma91yx.us-west-2.rds.amazonaws.com' dbname='kavdb' user='lkavenagh' password='" + pw + "'"
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(q)
    cursor.close()
    conn.close()
    
#%%
  
up = pd.read_csv('activities.csv')
up.columns = [c.lower().replace(' ', '_') for c in up.columns]
up = up[['date', 'activity_type', 'title', 'distance', 'calories', 'time', 'avg_hr', 'max_hr', 'avg_cadence', 'max_cadence',
         'avg_pace', 'best_pace', 'elev_gain', 'elev_loss', 'avg_stride_length']]
up.columns = ['date', 'activity_type', 'title', 'distance', 'calories', 'duration_seconds', 'avg_hr', 'max_hr', 'avg_cadence', 'max_cadence',
         'avg_pace', 'best_pace', 'elev_gain', 'elev_loss', 'avg_stride_length']
dbSendQuery("DELETE FROM runs.activities WHERE date >= '" + str(min(up.date)) + "' AND date <= '" + str(max(up.date)) + "'")

#%%

for i in range(len(up)):
    for col in ['duration_seconds', 'avg_pace', 'best_pace']:
        if len(up.loc[i, col].split(':')) == 2:
            up.loc[i, col] = (60*float(up.loc[i, col].split(':')[0])) + float(up.loc[i, col].split(':')[1])
        elif len(up.loc[i, col].split(':')) == 3:
            up.loc[i, col] = (60*60*float(up.loc[i, col].split(':')[0])) + (60*float(up.loc[i, col].split(':')[1])) + float(up.loc[i, col].split(':')[0])

up = up.replace('--', np.nan)

#%%
uploadDF(up, 'runs.activities')

#%%
os.remove('transactions.csv')