from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

# C:\Sudhakar\Projects\Youtube Data Harvesting\Youtube_Data_Harvesting>streamlit run youtube1.py

# API Key Connection


def APi_connect():
    Api_ID = "AIzaSyDKxTXNk8yWjLkDLpqLesZatoMmufOstao"
    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name, api_version, developerKey=Api_ID)

    return youtube


youtube = APi_connect()


# Get Channel information
def Get_channel_info(Channel_id):
    request = youtube.channels().list(
        part="snippet, contentDetails, statistics",
        id=Channel_id
    )
    response = request.execute()

    for item in response["items"]:
        Data = dict(Channel_name=item["snippet"]["title"],
                    Channel_id=item["id"],
                    Channel_description=item["snippet"]["description"],
                    Channel_subscriber_count=item["statistics"]["subscriberCount"],
                    Channel_playlist_id=item["contentDetails"]["relatedPlaylists"]["uploads"],
                    Channel_views_count=item["statistics"]["viewCount"],
                    Channel_video_count=item["statistics"]["videoCount"])
    return Data


# Get video ids
def Get_video_ids(Channel_id):
    video_ids = []
    response = youtube.channels().list(id=Channel_id,
                                       part="contentDetails").execute()
    playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(part=["snippet"],
                                                 playlistId=playlist_id,
                                                 maxResults=50,
                                                 pageToken=next_page_token).execute()
        for video_id in range(len(response1["items"])):
            video_ids.append(response1["items"][video_id]
                             ["snippet"]["resourceId"]["videoId"])
        next_page_token = response1.get("nextPageToken")

        if next_page_token is None:
            break
    return video_ids


# Get Video Information
def get_video_info(video_ids_list):
    video_data = []
    for video_id in video_ids_list:
        request = youtube.videos().list(
            part="contentDetails, snippet, statistics",
            id=video_id
        )
        response = request.execute()

        for item in response["items"]:
            data = dict(channel_Title=item['snippet']['channelTitle'],
                        channel_ID=item['snippet']['channelId'],
                        video_ID=item['id'],
                        title=item['snippet']['title'],
                        tags=item['snippet'].get('tags'),
                        thumbnails=item['snippet']['thumbnails']['default']['url'],
                        description=item['snippet']['description'],
                        published_At=item['snippet']['publishedAt'],
                        duration=item['contentDetails']['duration'],
                        view_Count=item['statistics'].get('viewCount'),
                        comment_Count=item['statistics'].get('commentCount'),
                        like_Count=item['statistics'].get('likeCount'),
                        favorite_Count=item['statistics']['favoriteCount'],
                        definition=item['contentDetails']['definition'],
                        caption=item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data


# Get comments information
def Get_Comment_Info(video_ids_list):
    commend_Data = []
    try:
        for video_id in video_ids_list:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response['items']:
                data = dict(comment_Id=item['snippet']['topLevelComment']['id'],
                            video_Id=item['snippet']['videoId'],
                            comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt']
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
            part="snippet, contentDetails",
            channelId=Channel_id,
            maxResults=50,
            pageToken=next_Page_Token
        )
        response = request.execute()

        for item in response['items']:
            data = dict(playlist_Id=item['id'],
                        title=item['snippet']['title'],
                        Channel_Id=item['snippet']['channelId'],
                        channel_Name=item['snippet']['channelTitle'],
                        published_At=item['snippet']['publishedAt'],
                        item_Count=item['contentDetails']['itemCount']
                        )
            playlist_info.append(data)

        next_Page_Token = response.get('pageToken')
        if next_Page_Token is None:
            break

    return playlist_info


# Make connection in Mongo DB
mongo_client = pymongo.MongoClient(
    "mongodb+srv://sudhakarsankar:sudhakar@cluster0.9udq9e7.mongodb.net/?retryWrites=true&w=majority")
mongo_db = mongo_client["youtube_data"]
mongo_collection = mongo_db['channel_Details']


def channel_Details(channel_Id):
    cha_Details = Get_channel_info(channel_Id)
    Play_List_Details = Get_Playlist_Detail(channel_Id)
    video_Id_List = Get_video_ids(channel_Id)
    video_Details = get_video_info(video_Id_List)
    com_Details = Get_Comment_Info(video_Id_List)

    mongo_collection.insert_one({"channel_Information": cha_Details, "Play_List_Information": Play_List_Details,
                                 "video_Information": video_Details, "comment_Information": com_Details})

    return "Details uploaded successfully"


def channel_Table():
    postgres_conn = psycopg2.connect(
        host='localhost',
        user='postgres',
        password='sudhakar',
        dbname='youtube_data',
        port=5432
    )
    postgres_cursor = postgres_conn.cursor()

    # Create table if it doesn't exist
    create_query = ''' 
    CREATE TABLE IF NOT EXISTS channels (
        Channel_name VARCHAR(100),
        Channel_id VARCHAR(50) PRIMARY KEY,
        Channel_description TEXT,
        Channel_subscriber_count BIGINT,
        Channel_playlist_id VARCHAR(50),
        Channel_views_count BIGINT,
        Channel_video_count INT
    )'''
    postgres_cursor.execute(create_query)
    postgres_conn.commit()
    print("Channel table is ready")

    # Read from MongoDB
    cha_List = []
    mongo_db = mongo_client["youtube_data"]
    mongo_collection = mongo_db['channel_Details']
    for cha_Data in mongo_collection.find({}, {"_id": 0, "channel_Information": 1}):
        cha_List.append(cha_Data['channel_Information'])
    df = pd.DataFrame(cha_List)

    # Insert or Update into PostgreSQL
    try:
        for index, row in df.iterrows():
            upsert_query = '''
            INSERT INTO channels (
                Channel_name,
                Channel_id,
                Channel_description,
                Channel_subscriber_count,
                Channel_playlist_id,
                Channel_views_count,
                Channel_video_count
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (Channel_id) DO UPDATE SET
                Channel_name = EXCLUDED.Channel_name,
                Channel_description = EXCLUDED.Channel_description,
                Channel_subscriber_count = EXCLUDED.Channel_subscriber_count,
                Channel_playlist_id = EXCLUDED.Channel_playlist_id,
                Channel_views_count = EXCLUDED.Channel_views_count,
                Channel_video_count = EXCLUDED.Channel_video_count;
            '''
            values = (
                row['Channel_name'],
                row['Channel_id'],
                row['Channel_description'],
                row['Channel_subscriber_count'],
                row['Channel_playlist_id'],
                row['Channel_views_count'],
                row['Channel_video_count']
            )
            postgres_cursor.execute(upsert_query, values)

        postgres_conn.commit()
        print("Channel data inserted/updated successfully")

    except Exception as e:
        print(f"Error inserting/updating data: {e}")

    finally:
        postgres_cursor.close()
        postgres_conn.close()


def playlist_Table():
    postgres_conn = psycopg2.connect(
        host='localhost',
        user='postgres',
        password='sudhakar',
        dbname='youtube_data',
        port=5432
    )
    postgres_cursor = postgres_conn.cursor()

    # Step 1: Create table if it doesn't exist
    create_query = ''' 
    CREATE TABLE IF NOT EXISTS playlists (
        playlist_Id VARCHAR(60) PRIMARY KEY,
        title VARCHAR(100),
        Channel_Id VARCHAR(60),
        channel_Name VARCHAR(100),
        published_At VARCHAR(100),
        item_Count INT
    )
    '''
    postgres_cursor.execute(create_query)
    postgres_conn.commit()
    print("Playlist table is ready.")

    # Step 2: Load data from MongoDB
    play_List = []
    mongo_db = mongo_client["youtube_data"]
    mongo_collection = mongo_db["channel_Details"]
    for playlist_Data in mongo_collection.find({}, {"_id": 0, "Play_List_Information": 1}):
        # More concise
        play_List.extend(playlist_Data["Play_List_Information"])

    df1 = pd.DataFrame(play_List)

    # Step 3: Insert or Update records
    try:
        for index, row in df1.iterrows():
            upsert_query = '''
            INSERT INTO playlists (
                playlist_Id,
                title,
                Channel_Id,
                channel_Name,
                published_At,
                item_Count
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (playlist_Id) DO UPDATE SET
                title = EXCLUDED.title,
                Channel_Id = EXCLUDED.Channel_Id,
                channel_Name = EXCLUDED.channel_Name,
                published_At = EXCLUDED.published_At,
                item_Count = EXCLUDED.item_Count;
            '''
            values = (
                row['playlist_Id'],
                row['title'],
                row['Channel_Id'],
                row['channel_Name'],
                row['published_At'],
                row['item_Count']
            )
            postgres_cursor.execute(upsert_query, values)

        postgres_conn.commit()
        print("Playlist data inserted/updated successfully.")

    except Exception as e:
        print(f"Error inserting/updating playlist data: {e}")

    finally:
        postgres_cursor.close()
        postgres_conn.close()


def videos_Table():
    try:
        # Step 1: Connect to PostgreSQL
        postgres_conn = psycopg2.connect(
            host='localhost',
            user='postgres',
            password='sudhakar',
            dbname='youtube_data',
            port=5432
        )
        postgres_cursor = postgres_conn.cursor()

        # Step 2: Create table if it doesn't exist
        create_query = '''
        CREATE TABLE IF NOT EXISTS videos (
            channel_Title VARCHAR(200),
            channel_ID VARCHAR(200),
            video_ID VARCHAR(100) PRIMARY KEY,
            title VARCHAR(300),
            tags TEXT,
            thumbnails VARCHAR(200),
            description TEXT,
            published_At TIMESTAMP,
            duration INTERVAL,
            view_Count BIGINT,
            comment_Count INT,
            like_Count INT,
            favorite_Count INT,
            definition VARCHAR(200),
            caption VARCHAR(300)
        )
        '''
        postgres_cursor.execute(create_query)
        postgres_conn.commit()
        print("videos' table is ready in PostgreSQL")

        # Step 3: Load video data from MongoDB
        video_List = []
        mongo_db = mongo_client["youtube_data"]
        mongo_collection = mongo_db["channel_Details"]

        for video_Data in mongo_collection.find({}, {"_id": 0, "video_Information": 1}):
            video_List.extend(video_Data.get("video_Information", []))

        df2 = pd.DataFrame(video_List)

        if df2.empty:
            print("No video data found in MongoDB.")
            return

        # Step 4: Insert or Update into PostgreSQL
        upsert_query = '''
        INSERT INTO videos (
            channel_Title, channel_ID, video_ID, title, tags, thumbnails,
            description, published_At, duration, view_Count, comment_Count,
            like_Count, favorite_Count, definition, caption
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (video_ID) DO UPDATE SET
            channel_Title = EXCLUDED.channel_Title,
            channel_ID = EXCLUDED.channel_ID,
            title = EXCLUDED.title,
            tags = EXCLUDED.tags,
            thumbnails = EXCLUDED.thumbnails,
            description = EXCLUDED.description,
            published_At = EXCLUDED.published_At,
            duration = EXCLUDED.duration,
            view_Count = EXCLUDED.view_Count,
            comment_Count = EXCLUDED.comment_Count,
            like_Count = EXCLUDED.like_Count,
            favorite_Count = EXCLUDED.favorite_Count,
            definition = EXCLUDED.definition,
            caption = EXCLUDED.caption;
        '''

        for index, row in df2.iterrows():
            values = (
                row.get('channel_Title'),
                row.get('channel_ID'),
                row.get('video_ID'),
                row.get('title'),
                row.get('tags'),
                row.get('thumbnails'),
                row.get('description'),
                row.get('published_At'),
                row.get('duration'),
                row.get('view_Count'),
                row.get('comment_Count'),
                row.get('like_Count'),
                row.get('favorite_Count'),
                row.get('definition'),
                row.get('caption')
            )
            postgres_cursor.execute(upsert_query, values)

        postgres_conn.commit()
        print("Video data inserted/updated successfully.")

    except Exception as e:
        print(f"Error occurred during video table processing: {e}")

    finally:
        if postgres_cursor:
            postgres_cursor.close()
        if postgres_conn:
            postgres_conn.close()
        print("PostgreSQL connection closed.")


def comments_Table():
    try:
        # Step 1: Connect to PostgreSQL
        postgres_conn = psycopg2.connect(
            host='localhost',
            user='postgres',
            password='sudhakar',
            dbname='youtube_data',
            port=5432
        )
        postgres_cursor = postgres_conn.cursor()

        # Step 2: Create comments table if not exists
        create_query = '''
        CREATE TABLE IF NOT EXISTS comments (
            comment_Id VARCHAR(100) PRIMARY KEY,
            video_Id VARCHAR(50),
            comment_Text TEXT,
            comment_Author VARCHAR(100),
            comment_Published TIMESTAMP
        )
        '''
        postgres_cursor.execute(create_query)
        postgres_conn.commit()
        print("'comments' table is ready in PostgreSQL.")

        # Step 3: Load data from MongoDB
        comment_List = []
        mongo_db = mongo_client["youtube_data"]
        mongo_collection = mongo_db["channel_Details"]

        for comment_Data in mongo_collection.find({}, {"_id": 0, "comment_Information": 1}):
            comment_List.extend(comment_Data.get("comment_Information", []))

        df3 = pd.DataFrame(comment_List)

        if df3.empty:
            print("No comment data found in MongoDB.")
            return

        # Step 4: Insert or update data
        upsert_query = '''
        INSERT INTO comments (
            comment_Id, video_Id, comment_Text, comment_Author, comment_Published
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (comment_Id) DO UPDATE SET
            video_Id = EXCLUDED.video_Id,
            comment_Text = EXCLUDED.comment_Text,
            comment_Author = EXCLUDED.comment_Author,
            comment_Published = EXCLUDED.comment_Published;
        '''

        for index, row in df3.iterrows():
            values = (
                row.get('comment_Id'),
                row.get('video_Id'),
                row.get('comment_Text'),
                row.get('comment_Author'),
                row.get('comment_Published')
            )
            postgres_cursor.execute(upsert_query, values)

        postgres_conn.commit()
        print("Comment data inserted/updated successfully.")

    except Exception as e:
        print(f"Error occurred during comment table processing: {e}")
        if postgres_conn:
            postgres_conn.rollback()

    finally:
        if postgres_cursor:
            postgres_cursor.close()
        if postgres_conn:
            postgres_conn.close()
        print("PostgreSQL connection closed.")


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
    for cha_Data in mongo_collection.find({}, {"_id": 0, "channel_Information": 1}):
        cha_List.append(cha_Data['channel_Information'])
    df = st.dataframe(cha_List)

    return df


def view_Playlist_Table():
    play_List = []
    mongo_db = mongo_client["youtube_data"]
    mongo_collection = mongo_db["channel_Details"]
    # channel count
    for playlist_Data in mongo_collection.find({}, {"_id": 0, "Play_List_Information": 1}):
        for i in range(len(playlist_Data["Play_List_Information"])):
            play_List.append(playlist_Data['Play_List_Information'][i])
    df1 = st.dataframe(play_List)

    return df1


def view_Video_Table():
    video_List = []
    mongo_db = mongo_client["youtube_data"]
    mongo_collection = mongo_db["channel_Details"]
    # channel count
    for video_Data in mongo_collection.find({}, {"_id": 0, "video_Information": 1}):
        for i in range(len(video_Data["video_Information"])):
            video_List.append(video_Data['video_Information'][i])
    df2 = st.dataframe(video_List)

    return df2


def view_Comment_Table():
    comment_List = []
    mongo_db = mongo_client["youtube_data"]
    mongo_collection = mongo_db["channel_Details"]
    # channel count
    for comment_Data in mongo_collection.find({}, {"_id": 0, "comment_Information": 1}):
        for i in range(len(comment_Data["comment_Information"])):
            comment_List.append(comment_Data['comment_Information'][i])
    df3 = st.dataframe(comment_List)

    return df3


# Set the page configuration to use the full width of the page
st.set_page_config(
    page_title="YouTube Data Harvesting & Warehousing",  # Optional: Custom Page Title
    layout="wide"  # This enables the wide layout to use the entire screen
)

# 🎯 App Title
st.title(":green[YouTube Data Harvesting & Warehousing]")

# ➕ Section 1: MongoDB Input
st.header(":blue[Store YouTube Channel Data in MongoDB]")
channel_ID = st.text_input("Enter YouTube Channel ID")

if st.button("📦 Store Channel in MongoDB"):
    if not channel_ID:
        st.warning("Please enter a Channel ID.")
    else:
        channel_ids = []
        mongo_db = mongo_client['youtube_data']
        mongo_collection = mongo_db['channel_Details']
        for channel_data in mongo_collection.find({}, {'_id': 0, 'channel_Information': 1}):
            channel_ids.append(
                channel_data['channel_Information']['Channel_id'])

        if channel_ID in channel_ids:
            st.success("✅ Channel ID already exists in MongoDB.")
        else:
            insert = channel_Details(channel_ID)
            st.success(insert)

# ➕ Section 2: Migrate to SQL
st.header(":orange[Migrate Data to PostgreSQL]")
if st.button("📦 Migrate to PostgreSQL"):
    SQL_Table = Tables()
    st.success(SQL_Table)

# ➕ Section 3: View Tables
st.header(":red[View SQL Tables]")
view_Table = st.radio("Choose a table to view", ("CHANNEL",
                      "PLAYLIST", "VIDEO", "COMMENT"), horizontal=True)

if view_Table == "CHANNEL":
    view_Channel_Table()
elif view_Table == "PLAYLIST":
    view_Playlist_Table()
elif view_Table == "VIDEO":
    view_Video_Table()
elif view_Table == "COMMENT":
    view_Comment_Table()

# ➕ Section 4: SQL Query Viewer
st.header(":red[Explore SQL Data with Questions]")

# PostgreSQL connection
postgres_conn = psycopg2.connect(
    host='localhost',
    user='postgres',
    password='sudhakar',
    dbname='youtube_data',
    port=5432
)
postgres_cursor = postgres_conn.cursor()

# Dropdown for selecting a question
question = st.selectbox("🧠 Choose a SQL Question", (
    "1. What are the names of all the videos and their corresponding channels?",
    "2. Which channels have the most number of videos, and how many videos do they have?",
    "3. What are the top 10 most viewed videos and their respective channels?",
    "4. How many comments were made on each video, and what are their corresponding video names?",
    "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
    "6. What is the total number of likes for each video, and what are their corresponding video names?",
    "7. What is the total number of views for each channel, and what are their corresponding channel names?",
    "8. What are the names of all the channels that have published videos in the year 2022?",
    "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
))

# 🧠 Query executor


def show_query_result(query, columns, transform_duration=False):
    try:
        postgres_cursor.execute(query)
        data = postgres_cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)

        if transform_duration and 'AverageDuration' in df.columns:
            df['AverageDuration'] = df['AverageDuration'].astype(str)

        st.dataframe(df, use_container_width=True)

    except Exception as e:
        st.error(f"An error occurred: {e}")


# 🚀 Execute selected query
if question.startswith("1"):
    show_query_result(
        "SELECT title AS videos, channel_Title AS channelName FROM videos",
        ['Video Title', 'Channel Name']
    )
elif question.startswith("2"):
    show_query_result(
        "SELECT Channel_name, Channel_video_count FROM channels ORDER BY Channel_video_count DESC",
        ['Channel Name', 'Total Videos']
    )
elif question.startswith("3"):
    show_query_result(
        "SELECT view_Count, channel_Title, title FROM videos WHERE view_Count IS NOT NULL ORDER BY view_Count DESC LIMIT 10",
        ['View Count', 'Channel Name', 'Video Title']
    )
elif question.startswith("4"):
    show_query_result(
        "SELECT comment_Count, title FROM videos WHERE comment_Count IS NOT NULL",
        ['Comment Count', 'Video Title']
    )
elif question.startswith("5"):
    show_query_result(
        "SELECT like_Count, title, channel_Title FROM videos WHERE like_Count IS NOT NULL ORDER BY like_Count DESC",
        ['Like Count', 'Video Title', 'Channel Title']
    )
elif question.startswith("6"):
    show_query_result(
        "SELECT like_Count, title FROM videos WHERE like_Count IS NOT NULL ORDER BY like_Count DESC",
        ['Like Count', 'Video Title']
    )
elif question.startswith("7"):
    show_query_result(
        "SELECT Channel_views_count, Channel_name FROM channels",
        ['Channel View Count', 'Channel Name']
    )
elif question.startswith("8"):
    show_query_result(
        "SELECT channel_Title, title, published_At FROM videos WHERE EXTRACT(YEAR FROM published_At) = 2022",
        ['Channel Title', 'Video Title', 'Published Date']
    )
elif question.startswith("9"):
    show_query_result(
        "SELECT channel_Title AS ChannelTitle, AVG(duration) AS AverageDuration FROM videos GROUP BY channel_Title",
        ['ChannelTitle', 'AverageDuration'],
        transform_duration=True
    )
elif question.startswith("10"):
    show_query_result(
        "SELECT channel_Title, title, comment_Count FROM videos WHERE comment_Count IS NOT NULL ORDER BY comment_Count DESC",
        ['Channel Title', 'Video Title', 'Comment Count']
    )
