import os
import sys
import psycopg2

import requests

import datetime

import pandas as pd

os.chdir(r'C:\users\lkave\documents\github\runs')

base_url = r'https://www.strava.com/api/v3/'

#%%
def readConfig(key):
    config = pd.read_csv(r'C:\users\lkave\documents\github\config.txt', header = None)
    config = [c.split('=') for c in config[0]]
    out = [c[1] for c in config if c[0] == key][0]
    return(out)

def addLineToConfig(key, val):
    with open(r'C:\users\lkave\documents\github\config.txt', 'a') as myfile:
        myfile.write('{}={}\n'.format(key, val))
        
def removeLineFromConfig(key):
    with open(r'C:\users\lkave\documents\github\config.txt', 'r') as myfile:
        lines = myfile.readlines()
    
    contents = dict()
    for line in lines:
        contents[line.split('=')[0]] = line.split('=')[1]
        
    with open(r'C:\users\lkave\documents\github\config.txt', 'w') as myfile:
        for key_to_write, val_to_write in contents.items():
            if key_to_write != key:
                myfile.write('{}={}'.format(key_to_write, val_to_write))

def getActivityList(pg = 1, before = str(datetime.datetime.now().date() + datetime.timedelta(1)), after = str(datetime.date(1970,1,2))):
    print('Fetching activities from ' + after + ' to ' + before + ' (page ' + str(pg) + ')')
    url = base_url + r'athlete/activities'
    url = url + r'?before=' + str(datetime.datetime.strptime(before, '%Y-%m-%d').timestamp())
    url = url + r'&after=' + str(datetime.datetime.strptime(after, '%Y-%m-%d').timestamp())
    url = url + r'&page=' + str(pg)
    dat = requests.get(url, headers={"Authorization":"Bearer {}".format(readConfig('stravatoken'))}).json()
    print('Found ' + str(len(dat)) + ' activities')
    
    return(dat)
    
def dbGetQuery(q):
    pw = readConfig('pw') 
    conn_string = "host='kavdb.c9lrodma91yx.us-west-2.rds.amazonaws.com' dbname='kavdb' user='lkavenagh' password='" + pw + "'"
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    dat = pd.read_sql(q, conn)
    conn.close()
    return(dat)
    
def dbSendQuery(q):
    pw = readConfig('pw') 
    conn_string = "host='kavdb.c9lrodma91yx.us-west-2.rds.amazonaws.com' dbname='kavdb' user='lkavenagh' password='" + pw + "'"
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(q)
    cursor.close()
    conn.close()
    
def uploadDF(dat, table):
    q = 'INSERT INTO ' + table + ' (' + ",".join(dat.columns) + ") VALUES ('"
    q = q + "'),('".join(dat.astype(str).apply("','".join, axis=1))
    q = q + "')"
    q = q.replace("'NULL'", "NULL")
    pw = readConfig('pw')
    conn_string = "host='kavdb.c9lrodma91yx.us-west-2.rds.amazonaws.com' dbname='kavdb' user='lkavenagh' password='" + pw + "'"
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(q)
    cursor.close()
    conn.close()

#%% Refresh API token
url = 'https://www.strava.com/oauth/token'
code = '36fb8087458cccb8d9f867909d52f3b3597c8dfb'

params = {'client_id': readConfig('stravaclientid'),
          'client_secret': readConfig('stravasecret'),
          'grant_type': 'refresh_token',
          'refresh_token': readConfig('stravarefreshtoken')
         }

r = requests.post(url, params = params).json()

removeLineFromConfig('stravatoken')
removeLineFromConfig('stravarefreshtoken')
addLineToConfig('stravatoken', r['access_token'])
addLineToConfig('stravarefreshtoken', r['refresh_token'])

#%%
    
if len(sys.argv) > 1:
    after = sys.argv[1]
else:
    after = str(dbGetQuery("SELECT max(start_date) as maxdate FROM runs.activities").maxdate[0].date() - datetime.timedelta(7))
    
#%% Query Strava API
p = 1
cols = ['average_speed', 'average_cadence', 'distance', 'elapsed_time', 'elev_high', 'elev_low', 'end_lat', 'end_long',
                              'strava_id', 'kudos_count', 'location_city', 'location_country', 'location_state', 'manual',
                              'max_speed', 'moving_time', 'name', 'achievement_count', 'pr_count', 'start_date', 'start_date_local',
                              'start_lat', 'start_long', 'timezone', 'total_elevation_gain', 'type']
out = pd.DataFrame(columns = cols)

dat = getActivityList(1, after = after)

while len(dat) > 0:
    
    for entry in dat:
        if entry['start_latlng'] is None:
            start_lat = 'NULL'
            start_long = 'NULL'
        else:
            start_lat = entry['start_latlng'][0]
            start_long = entry['start_latlng'][1]
        
        if entry['end_latlng'] is None:
            end_lat = 'NULL'
            end_long = 'NULL'
        else:
            end_lat = entry['end_latlng'][0]
            end_long = entry['end_latlng'][1]
            
        if 'elev_high' in entry.keys():
            elev_high = entry['elev_high']
            elev_low = entry['elev_low']
        else:
            elev_high = 0
            elev_low = 0
            
        if entry['manual']:
            manual = 1
        else:
            manual = 0
            
        if 'average_cadence' in entry.keys():
            average_cadence = entry['average_cadence']
        else:
            average_cadence = 'NULL'
            
        if entry['location_city'] is None:
            location_city = 'NULL'
        else:
            location_city = entry['location_city']
        
        if entry['location_country'] is None:
            location_country = 'NULL'
        else:
            location_country = entry['location_country']
            
        if entry['location_state'] is None:
            location_state = 'NULL'
        else:
            location_state = entry['location_state']
            
        tmp = pd.DataFrame([[
                entry['average_speed'],
                average_cadence,
                entry['distance'],
                entry['elapsed_time'],
                elev_high,
                elev_low,
                end_lat,
                end_long,
                entry['id'],
                entry['kudos_count'],
                location_city,
                location_country,
                location_state,
                manual,
                entry['max_speed'],
                entry['moving_time'],
                entry['name'],
                entry['achievement_count'],
                entry['pr_count'],
                entry['start_date'],
                entry['start_date_local'],
                start_lat,
                start_long,
                entry['timezone'],
                entry['total_elevation_gain'],
                entry['type'],
                ]], columns = cols)
        out = out.append(tmp)
    
    p += 1
    dat = getActivityList(p, after = after)

#%%
dbSendQuery("DELETE FROM runs.activities WHERE start_date >= '" + min(out.start_date) + "' AND start_date <= '" + max(out.start_date) + "'")
out.name = [c.replace("'", "") for c in out.name]
uploadDF(out, 'runs.activities')    
