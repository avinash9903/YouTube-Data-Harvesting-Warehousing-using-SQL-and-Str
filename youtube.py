from  googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#API key connection

def Api_connect():
    Api_Id="AIzaSyC7uiKclPszYlB29VyJHjz-jC8rOSHDDIs"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()

#get channels information
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

#get video ids
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

#get video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)    
    return video_data

#get comment information
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data

#get_playlist_details

def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                Video_Count=item['contentDetails']['itemCount'])
                        All_data.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data


client=pymongo.MongoClient("mongodb+srv://avinash:avinashg@cluster0.bx9w1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db=client["Youtube_data"]

def channel_details(channel_id):
    """
    Fetches channel details, playlist details, video details, and comment details for a given channel ID,
    and uploads them to the MongoDB collection.

    Parameters:
        channel_id (str): The ID of the YouTube channel.

    Returns:
        str: Success message.
    """
    try:
        # Fetch various details
        ch_details = get_channel_info(channel_id)
        pl_details = get_playlist_details(channel_id)
        vi_ids = get_videos_ids(channel_id)
        vi_details = get_video_info(vi_ids)
        com_details = get_comment_info(vi_ids)

        # Define the collection
        coll1 = db["channel_details"]

        # Insert the collected data into the collection
        coll1.insert_one({
            "channel_information": ch_details,
            "playlist_information": pl_details,
            "video_information": vi_details,
            "comment_information": com_details
        })

        return "Upload completed successfully"

    except Exception as e:
        return f"An error occurred: {e}"


#Table creation for channels,playlists,videos,comments
def channels_table(channel_name_s):
    """
    Creates a table for YouTube channels and inserts data for a specific channel
    if it does not already exist in the database.

    Parameters:
        channel_name_s (str): The name of the YouTube channel to insert.

    Returns:
        str: A message indicating the status of the operation.
    """
    try:
        with psycopg2.connect(host="localhost",
                              user="postgres",
                              password="avinashg",
                              database="youtube_data",
                              port="5432") as mydb:
            with mydb.cursor() as cursor:
                # Create the channels table if it does not exist
                create_query = '''
                CREATE TABLE IF NOT EXISTS channels (
                    Channel_Name VARCHAR(255),
                    Channel_Id VARCHAR(80) PRIMARY KEY,
                    Subscribers BIGINT,
                    Views BIGINT,
                    Total_Videos INT,
                    Channel_Description TEXT,
                    Playlist_Id VARCHAR(80)
                )
                '''
                cursor.execute(create_query)
                
                # Fetching all data
                cursor.execute("SELECT Channel_Name FROM channels")
                existing_channels = cursor.fetchall()
                existing_channel_names = [row[0] for row in existing_channels]

                if channel_name_s in existing_channel_names:
                    return f"Your Provided Channel '{channel_name_s}' already exists."

                # Fetching single channel details
                single_channel_details = []
                coll1 = db["channel_details"]
                for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_s}, {"_id": 0}):
                    single_channel_details.append(ch_data["channel_information"])

                if not single_channel_details:
                    return f"No details found for the channel '{channel_name_s}'."

                df_single_channel = pd.DataFrame(single_channel_details)

                # Insert the channel data into the table
                for index, row in df_single_channel.iterrows():
                    insert_query = '''
                    INSERT INTO channels (Channel_Name, Channel_Id, Subscribers, Views, Total_Videos, Channel_Description, Playlist_Id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    '''
                    values = (row['Channel_Name'],
                              row['Channel_Id'],
                              row['Subscribers'],
                              row['Views'],
                              row['Total_Videos'],
                              row['Channel_Description'],
                              row['Playlist_Id'])

                    try:
                        cursor.execute(insert_query, values)
                    except psycopg2.IntegrityError:
                        mydb.rollback()  # Rollback on error
                        return f"Channel values for '{row['Channel_Name']}' are already inserted."
                    except Exception as e:
                        mydb.rollback()  # Rollback on error
                        return f"An error occurred while inserting channel data: {e}"

                mydb.commit()  # Commit after all insertions

    except Exception as e:
        return f"Database connection error: {e}"

    return "Upload completed successfully."

def playlist_table(channel_name_s):
    """
    Creates a table for YouTube playlists and inserts data for a specific channel's playlists.

    Parameters:
        channel_name_s (str): The name of the YouTube channel to retrieve playlists for.

    Returns:
        str: A message indicating the status of the operation.
    """
    try:
        with psycopg2.connect(host="localhost",
                              user="postgres",
                              password="avinashg",
                              database="youtube_data",
                              port="5432") as mydb:
            with mydb.cursor() as cursor:
                # Create the playlists table if it does not exist
                create_query = '''
                CREATE TABLE IF NOT EXISTS playlists (
                    Playlist_Id VARCHAR(100) PRIMARY KEY,
                    Title VARCHAR(255),
                    Channel_Id VARCHAR(100),
                    Channel_Name VARCHAR(255),
                    PublishedAt TIMESTAMP,
                    Video_Count INT
                )
                '''
                cursor.execute(create_query)

                # Fetch single channel details
                single_channel_details = []
                coll1 = db["channel_details"]
                for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_s}, {"_id": 0}):
                    single_channel_details.append(ch_data["playlist_information"])

                if not single_channel_details:
                    return f"No playlists found for channel '{channel_name_s}'."

                df_single_channel = pd.DataFrame(single_channel_details[0])

                # Insert the playlist data into the table
                for index, row in df_single_channel.iterrows():
                    insert_query = '''
                    INSERT INTO playlists (Playlist_Id, Title, Channel_Id, Channel_Name, PublishedAt, Video_Count)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (Playlist_Id) DO NOTHING
                    '''  # Prevent duplicate entries

                    values = (row['Playlist_Id'],
                              row['Title'],
                              row['Channel_Id'],
                              row['Channel_Name'],
                              row['PublishedAt'],
                              row['Video_Count'])

                    try:
                        cursor.execute(insert_query, values)
                    except Exception as e:
                        return f"An error occurred while inserting playlist data: {e}"

                mydb.commit()  # Commit after all insertions

    except Exception as e:
        return f"Database connection error: {e}"

    return "Playlists uploaded successfully."

db = client["Youtube_data"]
coll1 = db["channel_details"]
def videos_table(channel_name_s):

    """
    Creates a table for YouTube videos and inserts data for a specific channel's videos.

    Parameters:
        channel_name_s (str): The name of the YouTube channel to retrieve videos for.

    Returns:
        str: A message indicating the status of the operation.
    """
    try:
        with psycopg2.connect(host="localhost",
                              user="postgres",
                              password="avinashg",
                              database="youtube_data",
                              port="5432") as mydb:
            with mydb.cursor() as cursor:
                # Create the videos table if it does not exist
                create_query = '''
                CREATE TABLE IF NOT EXISTS videos (
                    Channel_Name VARCHAR(100),
                    Channel_Id VARCHAR(100),
                    Video_Id VARCHAR(30) PRIMARY KEY,
                    Title VARCHAR(150),
                    Tags TEXT,
                    Thumbnail VARCHAR(200),
                    Description TEXT,
                    Published_Date TIMESTAMP,
                    Duration INTERVAL,
                    Views BIGINT,
                    Likes BIGINT,
                    Comments INT,
                    Favorite_Count INT,
                    Definition VARCHAR(10),
                    Caption_Status VARCHAR(50)
                )
                '''
                cursor.execute(create_query)

                # Fetch single channel details
                single_channel_details = []
                coll1 = db["channel_details"]
                for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_s}, {"_id": 0}):
                    single_channel_details.append(ch_data["video_information"])

                if not single_channel_details:
                    return f"No videos found for channel '{channel_name_s}'."

                df_single_channel = pd.DataFrame(single_channel_details[0])

                # Insert the video data into the table
                for index, row in df_single_channel.iterrows():
                    insert_query = '''
                    INSERT INTO videos (Channel_Name, Channel_Id, Video_Id, Title, Tags, Thumbnail, Description, 
                                        Published_Date, Duration, Views, Likes, Comments, Favorite_Count, 
                                        Definition, Caption_Status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (Video_Id) DO NOTHING  -- Prevent duplicate entries
                    '''

                    values = (row['Channel_Name'],
                              row['Channel_Id'],
                              row['Video_Id'],
                              row['Title'],
                              row['Tags'],
                              row['Thumbnail'],
                              row['Description'],
                              row['Published_Date'],
                              row['Duration'],
                              row['Views'],
                              row['Likes'],
                              row['Comments'],
                              row['Favorite_Count'],
                              row['Definition'],
                              row['Caption_Status'])

                    try:
                        cursor.execute(insert_query, values)
                    except Exception as e:
                        return f"An error occurred while inserting video data: {e}"

                mydb.commit()  # Commit after all insertions

    except Exception as e:
        return f"Database connection error: {e}"

    return "Videos uploaded successfully."
    
def comments_table(channel_name_s):
    """
    Creates a table for YouTube comments and inserts data for a specific channel's comments.

    Parameters:
        channel_name_s (str): The name of the YouTube channel to retrieve comments for.

    Returns:
        str: A message indicating the status of the operation.
    """
    try:
        with psycopg2.connect(host="localhost",
                              user="postgres",
                              password="avinashg",
                              database="youtube_data",
                              port="5432") as mydb:
            with mydb.cursor() as cursor:
                # Create the comments table if it does not exist
                create_query = '''
                CREATE TABLE IF NOT EXISTS comments (
                    Comment_Id VARCHAR(100) PRIMARY KEY,
                    Video_Id VARCHAR(50),
                    Comment_Text TEXT,
                    Comment_Author VARCHAR(150),
                    Comment_Published TIMESTAMP
                )
                '''
                cursor.execute(create_query)

                # Fetch single channel details
                single_channel_details = []
                coll1 = db["channel_details"]
                for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_s}, {"_id": 0}):
                    single_channel_details.append(ch_data["comment_information"])

                if not single_channel_details:
                    return f"No comments found for channel '{channel_name_s}'."

                df_single_channel = pd.DataFrame(single_channel_details[0])

                # Insert the comment data into the table
                for index, row in df_single_channel.iterrows():
                    insert_query = '''
                    INSERT INTO comments (Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (Comment_Id) DO NOTHING  -- Prevent duplicate entries
                    '''

                    values = (row['Comment_Id'],
                              row['Video_Id'],
                              row['Comment_Text'],
                              row['Comment_Author'],
                              row['Comment_Published'])

                    try:
                        cursor.execute(insert_query, values)
                    except Exception as e:
                        return f"An error occurred while inserting comment data: {e}"

                mydb.commit()  # Commit after all insertions

    except Exception as e:
        return f"Database connection error: {e}"

    return "Comments uploaded successfully."
def tables(channel_name):
    """
    Creates various tables (channels, playlists, videos, comments) based on the provided channel name.

    Parameters:
        channel_name (str): The name of the YouTube channel.

    Returns:
        str: A message indicating the status of the table creation.
    """
    try:
        news = channels_table(channel_name)
        
        if news:
            st.write(news)  # Display the news message if any
            return "Channel table already exists."

        # Create other tables if the channel table does not exist
        playlist_table(channel_name)
        videos_table(channel_name)
        comments_table(channel_name)

        return "All tables created successfully."

    except Exception as e:
        st.error(f"An error occurred during table creation: {e}")
        return "Table creation failed."
        
def show_channels_table():
    """
    Fetches channel information from the MongoDB 'channel_details' collection and displays it as a DataFrame.

    Returns:
        None: The function displays the DataFrame directly in the Streamlit app.
    """
    try:
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        
        # Fetch channel information directly into a DataFrame
        df = pd.DataFrame(list(coll1.find({}, {"_id": 0, "channel_information": 1})))

        if not df.empty:
            df = pd.json_normalize(df["channel_information"])  # Normalize JSON if needed
            st.dataframe(df)
        else:
            st.write("No channel information found.")

    except Exception as e:
        st.error(f"An error occurred while fetching channel data: {e}")

def show_playlists_table():
    """
    Fetches playlist information from the MongoDB 'channel_details' collection and displays it as a DataFrame.

    Returns:
        None: The function displays the DataFrame directly in the Streamlit app.
    """
    try:
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        
        # Fetch playlist information directly
        pl_data = list(coll1.find({}, {"_id": 0, "playlist_information": 1}))

        # Extracting playlists into a flat list
        pl_list = [pl for data in pl_data for pl in data.get("playlist_information", [])]

        # Convert to DataFrame
        df1 = pd.DataFrame(pl_list)

        if not df1.empty:
            st.dataframe(df1)
        else:
            st.write("No playlist information found.")

    except Exception as e:
        st.error(f"An error occurred while fetching playlist data: {e}")


def show_videos_table():
    """
    Fetches video information from the MongoDB 'channel_details' collection and displays it as a DataFrame.

    Returns:
        None: The function displays the DataFrame directly in the Streamlit app.
    """
    try:
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        
        # Fetch video information directly
        vi_data = list(coll1.find({}, {"_id": 0, "video_information": 1}))

        # Extracting videos into a flat list
        vi_list = [vi for data in vi_data for vi in data.get("video_information", [])]

        # Convert to DataFrame
        df2 = pd.DataFrame(vi_list)

        if not df2.empty:
            st.dataframe(df2)
        else:
            st.write("No video information found.")

    except Exception as e:
        st.error(f"An error occurred while fetching video data: {e}")


def show_comments_table():
    """
    Fetches comment information from the MongoDB 'channel_details' collection and displays it as a DataFrame.

    Returns:
        None: The function displays the DataFrame directly in the Streamlit app.
    """
    try:
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        
        # Fetch comment information directly
        com_data = list(coll1.find({}, {"_id": 0, "comment_information": 1}))

        # Extracting comments into a flat list
        com_list = [com for data in com_data for com in data.get("comment_information", [])]

        # Convert to DataFrame
        df3 = pd.DataFrame(com_list)

        if not df3.empty:
            st.dataframe(df3)
        else:
            st.write("No comment information found.")

    except Exception as e:
        st.error(f"An error occurred while fetching comment data: {e}")

# Streamlit sidebar
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

# Input for channel ID
channel_id = st.text_input("Enter the channel ID")

if st.button("Collect and Store Data"):
    # Check if the channel ID already exists
    existing_channel_ids = [ch_data["channel_information"]["Channel_Id"] for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1})]
    
    if channel_id in existing_channel_ids:
        st.success("Channel details of the given channel ID already exist.")
    else:
        insert = channel_details(channel_id)  # Make sure this function is defined elsewhere
        if insert:
            st.success("Channel details collected and stored successfully.")
        else:
            st.error("Failed to collect channel details.")

# New code to select a channel
all_channels= []
coll1=db["channel_details"]
for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
    all_channels.append(ch_data["channel_information"]["Channel_Name"])
        
unique_channel = st.selectbox("Select the Channel", all_channels)

if st.button("Migrate to SQL"):
    try:
        Table = tables(unique_channel)  # Make sure this function is defined
        st.success(f"Data migrated to SQL for channel: {unique_channel}.")
    except Exception as e:
        st.error(f"Error migrating to SQL: {e}")

# Table selection for viewing
show_table = st.radio("SELECT THE TABLE FOR VIEW", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

if show_table == "CHANNELS":
    show_channels_table()  # Ensure this function is defined
elif show_table == "PLAYLISTS":
    show_playlists_table()  # Ensure this function is defined
elif show_table == "VIDEOS":
    show_videos_table()  # Ensure this function is defined
elif show_table == "COMMENTS":
    show_comments_table()  # Ensure this function is defined



# SQL Connection
try:
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="avinashg",
        database="youtube_data",
        port="5432"
    )
    cursor = mydb.cursor()

    question = st.selectbox("Select your question", (
        "1. All the videos and the channel name",
        "2. Channels with most number of videos",
        "3. 10 most viewed videos",
        "4. Comments in each video",
        "5. Videos with highest likes",
        "6. Likes of all videos",
        "7. Views of each channel",
        "8. Videos published in the year of 2022",
        "9. Average duration of all videos in each channel",
        "10. Videos with highest number of comments"
    ))

    queries = {
        "1. All the videos and the channel name": "SELECT title AS videos, channel_name AS channelname FROM videos",
        "2. Channels with most number of videos": "SELECT channel_name AS channelname, total_videos AS no_videos FROM channels ORDER BY total_videos DESC",
        "3. 10 most viewed videos": "SELECT views AS views, channel_name AS channelname, title AS videotitle FROM videos WHERE views IS NOT NULL ORDER BY views DESC LIMIT 10",
        "4. Comments in each video": "SELECT comments AS no_comments, title AS videotitle FROM videos WHERE comments IS NOT NULL",
        "5. Videos with highest likes": "SELECT title AS videotitle, channel_name AS channelname, likes AS likecount FROM videos WHERE likes IS NOT NULL ORDER BY likes DESC",
        "6. Likes of all videos": "SELECT likes AS likecount, title AS videotitle FROM videos",
        "7. Views of each channel": "SELECT channel_name AS channelname, views AS totalviews FROM channels",
        "8. Videos published in the year of 2022": "SELECT title AS video_title, published_date AS videorelease, channel_name AS channelname FROM videos WHERE EXTRACT(YEAR FROM published_date) = 2022",
        "9. Average duration of all videos in each channel": "SELECT channel_name AS channelname, AVG(duration) AS averageduration FROM videos GROUP BY channel_name",
        "10. Videos with highest number of comments": "SELECT title AS videotitle, channel_name AS channelname, comments AS comments FROM videos WHERE comments IS NOT NULL ORDER BY comments DESC"
    }

    if question in queries:
        query = queries[question]
        try:
            cursor.execute(query)
            t = cursor.fetchall()
            if question == "1. All the videos and the channel name":
                df = pd.DataFrame(t, columns=["video title", "channel name"])
            elif question == "2. Channels with most number of videos":
                df = pd.DataFrame(t, columns=["channel name", "No of videos"])
            elif question == "3. 10 most viewed videos":
                df = pd.DataFrame(t, columns=["views", "channel name", "videotitle"])
            elif question == "4. Comments in each video":
                df = pd.DataFrame(t, columns=["no of comments", "videotitle"])
            elif question == "5. Videos with highest likes":
                df = pd.DataFrame(t, columns=["videotitle", "channelname", "likecount"])
            elif question == "6. Likes of all videos":
                df = pd.DataFrame(t, columns=["likecount", "videotitle"])
            elif question == "7. Views of each channel":
                df = pd.DataFrame(t, columns=["channel name", "totalviews"])
            elif question == "8. Videos published in the year of 2022":
                df = pd.DataFrame(t, columns=["videotitle", "published_date", "channelname"])
            elif question == "9. Average duration of all videos in each channel":
                df = pd.DataFrame(t, columns=["channelname", "averageduration"])
                df['averageduration'] = df['averageduration'].astype(str)
            elif question == "10. Videos with highest number of comments":
                df = pd.DataFrame(t, columns=["video title", "channel name", "comments"])

            st.write(df)

        except Exception as e:
            st.error(f"Query execution error: {e}")

finally:
    if cursor:
        cursor.close()
    if mydb:
        mydb.close()

