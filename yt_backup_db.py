#!/usr/bin/env python
# coding: utf-8

# In[1]:


# import libs and set SQL connection string
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
import sqlalchemy
import psycopg2
import pandas as pd
import numpy as np
import os
import time
from datetime import datetime
import glob


# In[7]:


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
        
 
# In[3]:

query = """
DROP table yt_reels_backup;
DROP table yt_channels2reels_backup;
DROP table yt_reels_stat_backup;
DROP table yt_channels_backup;
DROP table yt_channels_stat_backup;
CREATE table yt_reels_backup AS table yt_reels;
CREATE table yt_channels2reels_backup AS table yt_channels2reels;
CREATE table yt_reels_stat_backup AS table yt_reels_stat;
CREATE table yt_channels_backup AS table yt_channels;
CREATE table yt_channels_stat_backup AS table yt_channels_stat;
"""


# In[10]:
clean_flags(1)
try:
    conn_string = os.environ['PG_DB']
    db = create_engine(conn_string)
    conn = db.connect()
    rs = conn.execute(query)
    conn.close()		
    with open('ok1.txt', 'w') as fp:
     pass
    fp.close()
except:
    with open('error1.txt', 'w') as fp: 
     pass
    fp.close()
# In[5]:




# In[ ]:




