from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd 
import streamlit as st


#API Key Connection
def APi_connect():
    Api_ID = "AIzaSyDKxTXNk8yWjLkDLpqLesZatoMmufOstao"
    api_service_name = "youtube"
    api_version = "v3"
    
    youtube = build(api_service_name, api_version, developerKey=Api_ID)
    
    return youtube
youtube = APi_connect()



#Get Channel information
def Get_channel_info(Channel_id):
    request = youtube.channels().list(
        part = "snippet, contentDetails, statistics",
        id = Channel_id
    )               
    response = request.execute()

    for item in response["items"]:
        Data = dict(Channel_name = item["snippet"]["title"],
                    Channel_id = item["id"],
                    Channel_description = item["snippet"]["description"],
                    Channel_subscriber_count = item["statistics"]["subscriberCount"],
                    Channel_playlist_id = item["contentDetails"]["relatedPlaylists"]["uploads"],
                    Channel_views_count = item["statistics"]["viewCount"],
                    Channel_video_count = item["statistics"]["videoCount"])
    return Data


#Get video ids
def Get_video_ids(Channel_id):
    video_ids = []
    response = youtube.channels().list(id = Channel_id,
                                    part = "contentDetails").execute()
    playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token = None  

    while True:
        response1 = youtube.playlistItems().list(part = ["snippet"],
                                                playlistId = playlist_id,
                                                maxResults = 50,
                                                pageToken = next_page_token).execute()
        for video_id in range(len(response1["items"])):
            video_ids.append(response1["items"][video_id]["snippet"]["resourceId"]["videoId"])
        next_page_token = response1.get("nextPageToken")
        
        if next_page_token is None:
            break
    return video_ids



# Get Video Information 
def get_video_info(video_ids_list):
     video_data = []
     for video_id in video_ids_list:
          request = youtube.videos().list(
               part = "contentDetails, snippet, statistics",
               id = video_id
          )
          response = request.execute()
          
          for item in response["items"]:
               data = dict(channel_Title = item['snippet']['channelTitle'],
                         channel_ID = item['snippet']['channelId'],
                         video_ID = item['id'],
                         title = item['snippet']['title'],
                         tags = item['snippet'].get('tags'),   
                         thumbnails = item['snippet']['thumbnails']['default']['url'],
                         description = item['snippet']['description'],
                         published_At = item['snippet']['publishedAt'],
                         duration = item['contentDetails']['duration'],
                         view_Count = item['statistics'].get('viewCount'),
                         comment_Count = item['statistics'].get('commentCount'),
                         like_Count = item['statistics'].get('likeCount'),
                         favorite_Count = item['statistics']['favoriteCount'],
                         definition = item['contentDetails']['definition'],
                         caption = item['contentDetails']['caption'] 
                         )
               video_data.append(data)
     return video_data



# Get comments information
def Get_Comment_Info(video_ids_list):
    commend_Data = []
    try:
        for video_id in video_ids_list:
            request = youtube.commentThreads().list(
                part = 'snippet',
                videoId = video_id,
                maxResults = 50   
            )
            response = request.execute()

            for item in response['items']:
                data = dict(comment_Id = item['snippet']['topLevelComment']['id'],
                            video_Id = item['snippet']['videoId'],
                            comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_Published = item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                commend_Data.append(data)
    except:
        pass
    return commend_Data



# Get Playlist Information   
def Get_Playlist_Detail(Channel_id):
    next_Page_Token = None  
    playlist_info = []
    while True:
        request = youtube.playlists().list(
            part = "snippet, contentDetails",
            channelId = Channel_id,
            maxResults = 50,
            pageToken = next_Page_Token      
        )
        response = request.execute()

        for item in response['items']:
            data = dict(playlist_Id = item['id'],
                        title = item['snippet']['title'],
                        Channel_Id = item['snippet']['channelId'],
                        channel_Name = item['snippet']['channelTitle'],
                        published_At = item['snippet']['publishedAt'],    
                        item_Count = item['contentDetails']['itemCount']   
            )
            playlist_info.append(data)
            
        next_Page_Token = response.get('pageToken')
        if next_Page_Token is None:
            break
        
    return playlist_info



# Make connection in Mongo DB
mongo_client = pymongo.MongoClient("mongodb+srv://sudhakarsankar:sudhakar@cluster0.9udq9e7.mongodb.net/?retryWrites=true&w=majority")
mongo_db = mongo_client["youtube_data"]   
mongo_collection = mongo_db['channel_Details'] 



def channel_Details(channel_Id):
    cha_Details = Get_channel_info(channel_Id)
    Play_List_Details = Get_Playlist_Detail(channel_Id)
    video_Id_List = Get_video_ids(channel_Id)
    video_Details = get_video_info(video_Id_List)
    com_Details = Get_Comment_Info(video_Id_List)
    
    mongo_collection.insert_one({"channel_Information" : cha_Details, "Play_List_Information" : Play_List_Details,
                          "video_Information" : video_Details, "comment_Information" : com_Details})
    
    return "Details uploaded successfully"



# channel Table creation in postgres SQL
def channel_Table():
    postgres_conn = psycopg2.connect(host ='localhost',
                                        user ='postgres',
                                        password ='sudhakar',
                                        dbname ='youtube_data',
                                        port = 5432)
    postgres_cursor = postgres_conn.cursor()

    drop_query = '''DROP TABLE IF EXISTS channels'''
    postgres_cursor.execute(drop_query)
    postgres_conn.commit()

    try:
        create_query = ''' CREATE TABLE if not exists channels (Channel_name VARCHAR(100),
                                                                Channel_id VARCHAR(50) PRIMARY KEY,
                                                                Channel_description TEXT,
                                                                Channel_subscriber_count BIGINT,
                                                                Channel_playlist_id VARCHAR(50),
                                                                Channel_views_count BIGINT,
                                                                Channel_video_count INT)'''

        postgres_cursor.execute(create_query)
        postgres_conn.commit()
        print("Channel table created")
        
    except:
        print("Channel table already created")
        
        
    cha_List = []
    mongo_db = mongo_client["youtube_data"]   
    mongo_collection = mongo_db['channel_Details'] 
    for cha_Data in mongo_collection.find({}, {"_id":0, "channel_Information":1}):
        cha_List.append(cha_Data['channel_Information'])
    df = pd.DataFrame(cha_List)


        
    try:
    # Sample DataFrame 'df' assumed to exist
        for index, row in df.iterrows():
            insert_query = ''' INSERT INTO channels ( Channel_name,
                                                        Channel_id,
                                                        Channel_description,
                                                        Channel_subscriber_count,
                                                        Channel_playlist_id,
                                                        Channel_views_count,
                                                        Channel_video_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s) '''
            
            values = (row['Channel_name'],
                        row['Channel_id'],
                        row['Channel_description'],
                        row['Channel_subscriber_count'],
                        row['Channel_playlist_id'],
                        row['Channel_views_count'],
                        row['Channel_video_count'])
            
            # Execute the query and commit for each row
            
            postgres_cursor.execute(insert_query, values)
            postgres_conn.commit()

        print("Table values are inserted")

    except:
        print(f"Table values are already inserted")
    
    
    
# playlists Table creation in postgres SQL
def playlist_Table():
    postgres_conn = psycopg2.connect(host='localhost',
                                        user='postgres',
                                        password='sudhakar',
                                        dbname='youtube_data',
                                        port=5432)
    postgres_cursor = postgres_conn.cursor()

    drop_query = '''DROP TABLE IF EXISTS playlists'''
    postgres_cursor.execute(drop_query)
    postgres_conn.commit()

    create_query = '''CREATE TABLE if not exists playlists (playlist_Id VARCHAR(60) PRIMARY KEY,
                                                            title VARCHAR(100),
                                                            Channel_Id VARCHAR(60),
                                                            channel_Name VARCHAR(100),
                                                            published_At VARCHAR(100),
                                                            item_Count INT)'''
    postgres_cursor.execute(create_query)
    postgres_conn.commit()


    play_List = []
    mongo_db = mongo_client["youtube_data"]
    mongo_collection = mongo_db["channel_Details"]
    for playlist_Data in mongo_collection.find({}, {"_id":0, "Play_List_Information":1}):  # channel count
        for i in range(len(playlist_Data["Play_List_Information"])):
            play_List.append(playlist_Data['Play_List_Information'][i])
    df1 = pd.DataFrame(play_List)

    for index, row in df1.iterrows():
        insert_query = '''INSERT INTO playlists (playlist_Id,
                                                    title,
                                                    Channel_Id,
                                                    channel_Name,
                                                    published_At,
                                                    item_Count)
                        VALUES (%s, %s, %s, %s, %s, %s)'''

        values = (row['playlist_Id'],
                    row['title'],
                    row['Channel_Id'],
                    row['channel_Name'],
                    row['published_At'],
                    row['item_Count'])

        postgres_cursor.execute(insert_query, values)
        postgres_conn.commit()


# videos Table creation in SQL Server
def videos_Table():
    postgres_conn = psycopg2.connect(host='localhost',
                                        user='postgres',
                                        password='sudhakar',
                                        dbname='youtube_data',
                                        port=5432)
    postgres_cursor = postgres_conn.cursor()

    drop_query = '''DROP TABLE IF EXISTS videos'''
    postgres_cursor.execute(drop_query)
    postgres_conn.commit()

    create_query = '''CREATE TABLE if not exists videos (channel_Title varchar(200),
                                                            channel_ID varchar(200),
                                                            video_ID varchar(100) primary key,
                                                            title varchar(300),
                                                            tags text,   
                                                            thumbnails varchar(200),
                                                            description text,
                                                            published_At timestamp,
                                                            duration interval,
                                                            view_Count bigint,
                                                            comment_Count int,
                                                            like_Count int,
                                                            favorite_Count int,
                                                            definition varchar(200),
                                                            caption varchar(300))'''
    postgres_cursor.execute(create_query)
    postgres_conn.commit()

    video_List = []
    mongo_db = mongo_client["youtube_data"]
    mongo_collection = mongo_db["channel_Details"]
    for video_Data in mongo_collection.find({}, {"_id":0, "video_Information":1}):  # channel count
        for i in range(len(video_Data["video_Information"])):
            video_List.append(video_Data['video_Information'][i])
    df2 = pd.DataFrame(video_List) 


    for index, row in df2.iterrows():
        insert_query = '''INSERT INTO videos ( channel_Title,
                                                    channel_ID,
                                                    video_ID,
                                                    title,
                                                    tags,   
                                                    thumbnails,
                                                    description,
                                                    published_At,
                                                    duration,
                                                    view_Count,
                                                    comment_Count,
                                                    like_Count,
                                                    favorite_Count,
                                                    definition,
                                                    caption)
        
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)'''

        values = (row['channel_Title'],
                    row['channel_ID'],
                    row['video_ID'],
                    row['title'],
                    row['tags'],
                    row['thumbnails'],
                    row['description'],
                    row['published_At'],
                    row['duration'],
                    row['view_Count'],
                    row['comment_Count'],
                    row['like_Count'],
                    row['favorite_Count'],
                    row['definition'],
                    row['caption'])

        postgres_cursor.execute(insert_query, values)
        postgres_conn.commit()


# comments Table creation in postgres SQL
def comments_Table():
    postgres_conn = psycopg2.connect(host ='localhost',
                                        user ='postgres',
                                        password ='sudhakar',
                                        dbname ='youtube_data',
                                        port = 5432)
    postgres_cursor = postgres_conn.cursor()

    drop_query = '''DROP TABLE IF EXISTS comments'''
    postgres_cursor.execute(drop_query)
    postgres_conn.commit()

    create_query = '''CREATE TABLE if not exists comments (comment_Id varchar(100) primary key,
                                                            video_Id varchar(50),
                                                            comment_Text text,
                                                            comment_Author varchar(100),
                                                            comment_Published timestamp)'''
    postgres_cursor.execute(create_query)
    postgres_conn.commit()


    comment_List = []
    mongo_db = mongo_client["youtube_data"]
    mongo_collection = mongo_db["channel_Details"]
    for comment_Data in mongo_collection.find({}, {"_id":0, "comment_Information":1}):  # channel count
        for i in range(len(comment_Data["comment_Information"])):
            comment_List.append(comment_Data['comment_Information'][i])
    df3 = pd.DataFrame(comment_List)


    try:
        for index, row in df3.iterrows():
            insert_query = '''INSERT INTO comments (comment_Id, video_Id, comment_Text, comment_Author, comment_Published)
                            VALUES (%s, %s, %s, %s, %s)'''

            values = (row['comment_Id'], row['video_Id'], row['comment_Text'], row['comment_Author'], row['comment_Published'])

            postgres_cursor.execute(insert_query, values)
            postgres_conn.commit()

        print("Data inserted successfully")

    except Exception as e:
        postgres_conn.rollback()  # Rollback the transaction
        print("Error occurred:", e)
         
def Tables():
    channel_Table()
    playlist_Table()
    videos_Table()
    comments_Table()
    
    return 'Tables created successfully'
   
def view_Channel_Table():
    cha_List = []
    mongo_db = mongo_client["youtube_data"]   
    mongo_collection = mongo_db['channel_Details'] 
    for cha_Data in mongo_collection.find({}, {"_id":0, "channel_Information":1}):
        cha_List.append(cha_Data['channel_Information'])
    df = st.dataframe(cha_List)
    
    return df


def view_Playlist_Table():
    play_List = []
    mongo_db = mongo_client["youtube_data"]
    mongo_collection = mongo_db["channel_Details"]
    for playlist_Data in mongo_collection.find({}, {"_id":0, "Play_List_Information":1}):  # channel count
        for i in range(len(playlist_Data["Play_List_Information"])):
            play_List.append(playlist_Data['Play_List_Information'][i])
    df1 = st.dataframe(play_List)
    
    return df1


def view_Video_Table():
    video_List = []
    mongo_db = mongo_client["youtube_data"]
    mongo_collection = mongo_db["channel_Details"]
    for video_Data in mongo_collection.find({}, {"_id":0, "video_Information":1}):  # channel count
        for i in range(len(video_Data["video_Information"])):
            video_List.append(video_Data['video_Information'][i])
    df2 = st.dataframe(video_List) 
    
    return df2


def view_Comment_Table():
    comment_List = []
    mongo_db = mongo_client["youtube_data"]
    mongo_collection = mongo_db["channel_Details"]
    for comment_Data in mongo_collection.find({}, {"_id":0, "comment_Information":1}):  # channel count
        for i in range(len(comment_Data["comment_Information"])):
            comment_List.append(comment_Data['comment_Information'][i])
    df3 = st.dataframe(comment_List)
    
    return df3


# Streamlit 
st.header(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

        
channel_ID = st.text_input("Enter the channel ID")

if st.button("Store the data into MONGODB"):
    if not channel_ID:
        st.warning('Please enter the channel ID')
    else:
        channel_ids = []
        mongo_db = mongo_client['youtube_data']
        mongo_collection = mongo_db['channel_Details']
        for channel_data in mongo_collection.find({}, {'_id' : 0, 'channel_Information' : 1}):
            channel_ids.append(channel_data['channel_Information']['Channel_id'])

        if channel_ID in channel_ids:
            st.success('Channel ID already exists')
        else:
            insert = channel_Details(channel_ID)
            st.success(insert)

       
if st.button("Data Migrate to SQL"):
    SQL_Table = Tables()
    st.success(SQL_Table)
    
    
view_Table = st.radio("Select the table for view", ("CHANNEL", "PLAYLIST", "VIDEO", "COMMENT"))

if view_Table == "CHANNEL" :
    view_Channel_Table()
    
elif view_Table == "PLAYLIST" :
    view_Playlist_Table()
    
elif view_Table == "VIDEO" :
    view_Video_Table()
    
elif view_Table == "COMMENT" :
    view_Comment_Table()
    

# SQL Connection 
postgres_conn = psycopg2.connect(host ='localhost',
                                        user ='postgres',
                                        password ='sudhakar',
                                        dbname ='youtube_data',
                                        port = 5432)
postgres_cursor = postgres_conn.cursor()

question = st.selectbox("Select the question", ("1.	What are the names of all the videos and their corresponding channels?",
                                                "2.	Which channels have the most number of videos, and how many videos do they have?",
                                                "3.	What are the top 10 most viewed videos and their respective channels?",
                                                "4.	How many comments were made on each video, and what are their corresponding video names?",
                                                "5.	Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                "7.	What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8.	What are the names of all the channels that have published videos in the year 2022?",
                                                "9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

if question == "1.	What are the names of all the videos and their corresponding channels?" :
    query1 = '''select title as videos, channel_Title as channelName from videos'''
    postgres_cursor.execute(query1)
    postgres_conn.commit
    query1_Data = postgres_cursor.fetchall()
    df1 = pd.DataFrame(query1_Data, columns = ['video title', 'channel name'])
    st.write(df1)
    
elif question == "2.	Which channels have the most number of videos, and how many videos do they have?" :
    query2 = '''select Channel_name, Channel_video_count from channels
                order by Channel_video_count desc'''
    postgres_cursor.execute(query2)
    postgres_conn.commit()
    query2_Data = postgres_cursor.fetchall()
    df2 = pd.DataFrame(query2_Data, columns = ['channel name', 'total videos'])
    st.write(df2)
    
elif question == "3.	What are the top 10 most viewed videos and their respective channels?" :
    query3 = '''select view_Count, channel_Title, title from videos
                where view_Count is not null order by view_Count desc limit 10'''
    postgres_cursor.execute(query3)
    postgres_conn.commit()
    query3_Data = postgres_cursor.fetchall()
    df3 = pd.DataFrame(query3_Data, columns = ['View count', 'Channel name', 'Video title'])
    st.write(df3)
    
elif question == "4.	How many comments were made on each video, and what are their corresponding video names?" :
    query4 = '''select comment_Count, title from videos
                where comment_Count is not null'''
    postgres_cursor.execute(query4)
    postgres_conn.commit()
    query4_Data = postgres_cursor.fetchall()
    df4 = pd.DataFrame(query4_Data, columns = ['Comment count', 'Video title'])
    st.write(df4)
    
elif question == "5.	Which videos have the highest number of likes, and what are their corresponding channel names?" :
    query5 = '''select like_Count, title, channel_Title from videos
                where like_Count is not null order by like_Count desc'''
    postgres_cursor.execute(query5)
    postgres_conn.commit()
    query5_Data = postgres_cursor.fetchall()
    df5 = pd.DataFrame(query5_Data, columns = ['Like count', 'Video title', 'Channel title'])
    st.write(df5)
    
elif question == "6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?" :
    query6 = '''select like_Count, title from videos
                where like_Count is not null order by like_Count desc'''
    postgres_cursor.execute(query6)
    postgres_conn.commit()
    query6_Data = postgres_cursor.fetchall()
    df6 = pd.DataFrame(query6_Data, columns = ['Like count', 'Video title name'])
    st.write(df6)
    
elif question == "7.	What is the total number of views for each channel, and what are their corresponding channel names?" :
    query7 = '''select Channel_views_count, Channel_name from channels'''
    postgres_cursor.execute(query7)
    postgres_conn.commit()
    query7_Data = postgres_cursor.fetchall()
    df7 = pd.DataFrame(query7_Data, columns = ['Channel view count', 'Channel name'])
    st.write(df7)
    
elif question == "8.	What are the names of all the channels that have published videos in the year 2022?" :
    query8 = '''select channel_Title, title, published_At from videos
                where extract(year from published_At) = 2022'''
    postgres_cursor.execute(query8)
    postgres_conn.commit()
    query8_Data = postgres_cursor.fetchall()
    df8 = pd.DataFrame(query8_Data, columns = ['Channel title', 'Video title', 'Published date'])
    st.write(df8)
        
elif question == "9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?" :
    query9 = '''select channel_Title, AVG(duration) as averageDuration from videos
                group by channel_Title'''
    postgres_cursor.execute(query9)
    postgres_conn.commit()
    query9_Data = postgres_cursor.fetchall()
    df9 = pd.DataFrame(query9_Data, columns = ['ChannelTitle', 'AverageDuration'])

    # Average duration not displaying in streamlit so 
    query9_Data = []
    for index, row in df9.iterrows():
        cha_Title = row['ChannelTitle']
        avg_Duration = row['AverageDuration']
        avg_Duration_STR = str(avg_Duration)
        query9_Data.append(dict(ChannelTitle = cha_Title, AverageDuration = avg_Duration_STR))
    df = pd.DataFrame(query9_Data) 
    st.write(df)
    
elif question == "10. Which videos have the highest number of comments, and what are their corresponding channel names?" :
    query10 = '''select channel_Title, title, comment_Count from videos
                where comment_Count is not null order by comment_Count desc'''
    postgres_cursor.execute(query10)
    postgres_conn.commit()
    query10_Data = postgres_cursor.fetchall()
    df10 = pd.DataFrame(query10_Data, columns = ['Channel title', 'Video title', 'Comment count'])
    st.write(df10)
    
    





