#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
from apiclient.discovery import build
import pandas as pd
import time
import datetime
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
from tqdm import tqdm


# In[2]:


conn_string = os.environ['PG_DB']
db = create_engine(conn_string)
conn = db.connect()

def clean_flags(level):
    filename = "ok"+str(level)+".txt"
    try:
        os.remove(filename)
    except OSError:
        pass
    filename = "error"+str(level)+".txt"
    try:
        os.remove(filename)
    except OSError:
        pass

inject_dict = { 'yt_channel_id': sqlalchemy.types.TEXT(),  
                'ch_views': sqlalchemy.types.BIGINT(), 
                'ch_subscribers': sqlalchemy.types.BIGINT(),         
                'ch_videos': sqlalchemy.types.BIGINT(),        
                'data_date': sqlalchemy.types.Date(),
                }
start_time = time.time()
clean_flags(2)

try:
    df = pd.read_csv("./data/channel_stat.csv",  parse_dates=['data_date'])
    res = df.to_sql('yt_channels_stat', con=conn, if_exists='append', index=False, dtype = inject_dict)
    print("to_sql duration: {} seconds".format(time.time() - start_time))
    with open('ok2.txt', 'w') as fp:
     pass
    fp.close()
except:
    with open('error2.txt', 'w') as fp: 
     pass
    fp.close()


# In[ ]:




