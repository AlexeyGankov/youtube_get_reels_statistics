#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import sys
from apiclient.discovery import build
import pandas as pd
import datetime
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
from tqdm import tqdm


# In[2]:


conn_string = os.environ['PG_DB']
api_key = os.environ['G_API']
youtube = build('youtube', 'v3', developerKey=api_key)
print(conn_string)
print(api_key)


# In[8]:

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
        
            
def get_channel_videos(channel_id):
    print("Work with channel:", channel_id)
    res = youtube.channels().list(id=channel_id, part = 'contentDetails,statistics').execute()
    #print(res)
    videos=[]
    try:
        playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        ch_views = res['items'][0]['statistics']['viewCount']
        ch_subscribers = res['items'][0]['statistics']['subscriberCount']
        ch_videos = res['items'][0]['statistics']['videoCount']
        print("Ch_views:",ch_views, " Ch_subscribers:",ch_subscribers," Ch_videos:",ch_videos)
        next_page_token = None
        while 1:
            res2 = youtube.playlistItems().list(playlistId=playlist_id, part='snippet',
                                          maxResults=50,
                                          pageToken=next_page_token).execute()
            print("** ", end =" ")
            #print(res2)
            videos += res2['items']
            try:
                next_page_token = res2['nextPageToken']
                print(res2['nextPageToken'], end =" ")
            except:
                break
            if next_page_token is None:
                break
        return videos, ch_views, ch_subscribers, ch_videos
    except:
        return videos,-1,-1,-1


# In[9]:


def get_video_info(v_id):
    final=[]
    for i in tqdm(range(0, len(v_id), 50)):
        v_ids = ','.join(v_id[i:i + 50])
        res3 = youtube.videos().list(part="snippet,contentDetails,statistics",
        id=v_ids).execute()
        final += res3['items']
    return(final)


# In[10]:


def make_table(final,ch_views, ch_subscribers, ch_videos ):
    result=[]
    for r in final:
#    print(r['id'],r['snippet']['publishedAt'],r['snippet']['description'],r['snippet']['title'], r['statistics']['viewCount'],r['statistics']['likeCount'] )
        Social ='YT'
        Url = 'https://www.youtube.com/watch?v='+r['id']
        try:
            Likes = int(r['statistics']['likeCount'])
        except:
            Likes = 0
        try:
            Comments = int(r['statistics']['commentCount'])
        except:
            Comments = 0
        Views = int(r['statistics']['viewCount'])
        if float(ch_subscribers) != 0:
            ER = (Likes+Comments)/(float(ch_subscribers))*100.
        else:
            ER = 0
        VR=3.1415
        Text = "["+r['snippet']['title']+"] "+r['snippet']['description']
        Date = pd.to_datetime(r['snippet']['publishedAt'],  errors='ignore', utc=True)
        Date = Date.tz_localize(None)
        Author = " "
        Media1 = 'video'
        duration = r['contentDetails']['duration']
        if Views != 0:
            yr = (Likes+Comments)/Views*100.
        else:
            yr = 0
            print("No views URL", Url)
        broadcast = r['snippet']['liveBroadcastContent']
   # result.append([r['id'],r['snippet']['publishedAt'],r['snippet']['description'],r['snippet']['title'], r['statistics']['viewCount'],r['statistics']['likeCount'], r['statistics']['commentCount'], r['contentDetails']['duration']] )
        result.append([Social, Url,Likes,Comments,Views,ER, VR,Text,Date,Author,Media1, duration, yr, broadcast])
    return(result)


# In[11]:


#################

clean_flags(0)
# In[12]:

try:
    print("work with sql : yt_channels")
    db = create_engine(conn_string)
    conn = db.connect()
    yt_ch = pd.read_sql('SELECT yt_channel_id FROM  yt_channels where flag_closed = 0', conn)
    lines = yt_ch.values.tolist()

    
    # In[13]:
    
    
    print("Number of youtube channels: ",len(lines))
    
    
    # In[14]:
    
    
    fname = './data/errors_report.txt'
    fp_err=open(fname, "wt")
    ch_stat=[]
    counter = 1
    for id in lines:
        print("======= ",counter," ===========", end =" ")    
        print(id[0])
        counter += 1
        videos, ch_views, ch_subscribers, ch_videos = get_channel_videos(id[0])
        ch_stat.append([id[0], ch_views, ch_subscribers, ch_videos])
        if len(videos)>0:
            v_id = []
            for video in videos:
                v_id.append(video['snippet']['resourceId']['videoId'])
            print("\n Number of videos on channel:",len(v_id))
            res = get_video_info(v_id)
            print("Number of videos get info:",len(res))
            res2 = make_table(res,ch_views, ch_subscribers, ch_videos )
            df = pd.DataFrame(res2)
            df.columns=['Social', 'Url','Likes','Comments','Views','ER', 'VR','Text','Date','Author','Media 1','Duration', 'YR', 'Broadcast']
            fname='./data/'+id[0]+'.xlsx'
            df.to_excel(fname, index=False)
            print("Writing file ", fname, " - done")
        else:
            fp_err.write("no data:" + id[0]+"\n")
    fp_err.close()
    
    
    # In[15]:
    
    
    ch_df = pd.DataFrame(ch_stat)
    ch_df.columns= ['yt_channel_id', 'ch_views', 'ch_subscribers', 'ch_videos']
    ch_df['data_date'] = datetime.datetime.today().date()
    fname='./data/channel_stat.csv'
    ch_df.to_csv(fname,index=False)
    with open('ok0.txt', 'w') as fp: 
     pass
    fp.close()
except:
    with open('error0.txt', 'w') as fp: 
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fp.write(str(exc_type) + '\n')
        fp.write(str(exc_obj)+ '\n')
        fp.write(str(exc_tb)+ '\n')
    fp.close()
    


