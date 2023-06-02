# Import necessary packages
import streamlit as st
import requests_cache
import sqlite3
from googleapiclient.discovery import build
from pymongo import MongoClient
import pandas as pd
import datetime
# import zlib
# import json

# Configure page for streamlit
st.set_page_config(layout='wide')
requests_cache.install_cache('youtube_cache', expire_after=3600)
# Use the api get from GCP
api_key = "AIzaSyCTq8fH_Zwmw7dWZNj3-DIDyw4wEFPqnd0"
youtube = build('youtube', 'v3', developerKey=api_key)

# Create two columns for displaying the output
c1, c2 = st.columns([6, 6])

with c2:
    c_id = st.text_input("Enter the channel id:", "UCjW5u7vHnvwuXiVQx2ahs4A")

# Create a variable to retrieve channel data using API
ch_search = youtube.channels().list(
            id=c_id, part='snippet,statistics,status').execute()


def channel_data(response):
    channel_name = response['items'][0]['snippet']['title']
    channel_id = response['items'][0]['id']
    channel_views = response['items'][0]['statistics']['viewCount']
    channel_description = response['items'][0]['snippet']['description']
    channel_status = response['items'][0]['status']['privacyStatus']
    subscriber_count = response['items'][0]['statistics']['subscriberCount']

    channel_dic = {
        "Name of the channel": channel_name,
        "Id": channel_id,
        "Views": channel_views,
        "Subscribers": subscriber_count,
        "Description": channel_description,
        "Status": channel_status
    }
    return channel_dic


# Use API to retrieve playlist data
def playlist_data(response):
    playlists = []
    next_page_token = None
    while True:
        playlist_response = youtube.playlists().list(part='snippet,contentDetails,id',
                                                     maxResults=50, channelId=response,
                                                     pageToken=next_page_token).execute()
        next_page_token = playlist_response.get("nextPageToken")
        for play in playlist_response["items"]:
            try:
                playlists.append({
                    "Playlist_id": play["id"],
                    "Id": play["snippet"]["channelId"],
                    "Playlist_title": play["snippet"]["title"],
                    "Playlists_count": play["contentDetails"]["itemCount"]
                })

            except Exception as e:
                print(f'Error retrieving comments for video {"Playlist_id"}: {e}')
                continue
        if not next_page_token:
            break
    return playlists


# Use dataframe to store it as a table
play_lists = pd.DataFrame(playlist_data(c_id)).copy()
play_list = play_lists.to_dict('records').copy()
pl = play_lists["Playlist_id"].tolist().copy()


# Function to get videos in a playlist
def get_playlist_videos(playlist_id):
    vis = []
    next_page_token = None
    while True:
        # get playlist items
        res = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        # add videos to list
        for item in res['items']:
            video_id = item['snippet']['resourceId']['videoId']
            video = {
                'playlist_id': playlist_id,
                'Video ID': video_id
            }
            vis.append(video)

        # check if there are more pages
        next_page_token = res.get('nextPageToken')
        if not next_page_token:
            break

    return vis


# Function to get videos in multiple playlists
def get_videos_in_playlists(playlists_ids):
    vid = []
    for playlist_id in playlists_ids:
        playlist_videos = get_playlist_videos(playlist_id)
        vid.extend(playlist_videos)
    return vid


# Convert video ids in dataframe to list
pl_lists = pd.DataFrame(get_videos_in_playlists(pl)).copy()
pl_videos = pl_lists["Video ID"].tolist().copy()


# Function to retrieve video details for a list of video IDs
def video_data(video_ids):
    videos = []

    # Retrieve video details in batches of 50 using pagination
    for i in range(0, len(video_ids), 50):
        video_ids_batch = video_ids[i:i + 50]

        video_response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(video_ids_batch)
        ).execute()

        for video_result in video_response.get("items", []):
            video_id = video_result["id"]

            if video_id in video_ids:
                video_title = video_result["snippet"]["title"]
                video_description = video_result["snippet"]["description"]
                published_at = datetime.datetime.strptime(video_result["snippet"]["publishedAt"], "%Y-%m-%dT%H:%M:%SZ")
                tags = video_result["snippet"].get("tags", [])
                view_count = video_result["statistics"]["viewCount"]
                like_count = video_result["statistics"]["likeCount"]
                favorite_count = video_result["statistics"]["favoriteCount"]
                comment_count = video_result["statistics"].get("commentCount", 0)
                duration = video_result["contentDetails"]["duration"]
                thumbnail = video_result["snippet"]["thumbnails"]["default"]["url"]
                caption_status = video_result["contentDetails"]["caption"]

                videos.append({
                    "Video ID": video_id,
                    "Title": video_title,
                    "Description": video_description,
                    "Published At": published_at,
                    "Tags": tags,
                    "View Count": view_count,
                    "Like Count": like_count,
                    "Favorite Count": favorite_count,
                    "Comment Count": comment_count,
                    "Duration": duration,
                    "Thumbnail URL": thumbnail,
                    "Caption Status": caption_status
                })
    return videos


# Store the video data in dataframe
df = pd.DataFrame(video_data(pl_videos))
df_videos = df.copy()
# Check for any missing video in tables
original_video_ids = set(pl_lists["Video ID"])
retrieved_video_ids = set(df["Video ID"])
missing_id = original_video_ids - retrieved_video_ids
missing = pl_lists[pl_lists["Video ID"].isin(missing_id)]
pl_lists.drop(missing.index, inplace=True)


# Retrieve comments data from youtube
def comments_data():
    comment_data = []
    for index, row in df_videos.iterrows():
        try:
            comments_response = youtube.commentThreads().list(part='snippet', videoId=row["Video ID"],
                                                              maxResults=15, textFormat='plainText').execute()

            for comment in comments_response['items']:
                author_name = comment['snippet']['topLevelComment']['snippet']["authorDisplayName"]
                comment_id = comment['id']
                comment_text = comment['snippet']['topLevelComment']['snippet']["textDisplay"]
                published_time = datetime.datetime.strptime(
                    comment['snippet']['topLevelComment']['snippet']["publishedAt"], "%Y-%m-%dT%H:%M:%SZ"
                )
                comment_data.append(
                    {"Video ID": row["Video ID"],
                     "Comment ID": comment_id,
                     "Author name": author_name,
                     "Comment": comment_text,
                     "Published at": published_time}
                )
        except Exception as e:
            print(f'Error retrieving comments for video {row["Video ID"]}: {e}')
            continue
    return comment_data


comment_df = pd.DataFrame(comments_data()).copy()
comment_df['Published at'] = comment_df['Published at'].dt.strftime("%Y-%m-%d %H:%M:%S")
comment_df.drop_duplicates(subset='Comment ID', keep='first', inplace=True)
# Create columns to display the result
c3, c4 = st.columns([4, 12])

header_color = "#FF0000"
with c3:
    for key, value in channel_data(ch_search).items():
        st.markdown(f"<h5 style='color:{header_color}'>{key}</h5>", unsafe_allow_html=True)
        st.write(value)

with c4:
    st.write(pd.DataFrame(playlist_data(c_id)))
    # st.write(pd.DataFrame(get_videos_in_playlists(pl)))
    st.write(pd.DataFrame(video_data(pl_videos)))

comment_list = comments_data()
pi_video = pl_lists['playlist_id']
fi_video = df.copy()
position = 0
# Insert a column with playlist id in video table
fi_video.insert(position, 'playlist_id', pi_video)
fi_video['Published At'] = fi_video['Published At'].dt.strftime("%Y-%m-%d %H:%M:%S")
f_video = fi_video.to_dict('records').copy()

# Create a dictionary to store in mongodb
channel_dict = {
    'id': channel_data(ch_search)['Id'],
    'name': channel_data(ch_search)['Name of the channel'],
    'views': channel_data(ch_search)['Views'],
    'subscribers': channel_data(ch_search)['Subscribers'],
    'description': channel_data(ch_search)['Description'],
    'status': channel_data(ch_search)['Status'],
    'playlists': []
}
# loop over each playlist
for playlist in play_list:
    # create a nested dictionary for the playlist
    playlist_dict = {
        'Playlist id': playlist['Playlist_id'],
        'Playlist name': playlist['Playlist_title'],
        'video_count': playlist['Playlists_count'],
        'videos': []
    }
    # loop over each video in the playlist
    for vi in f_video:
        if vi['playlist_id'] == playlist['Playlist_id']:
            # create a nested dictionary for the video
            video_dict = {'Title': vi['Title'],
                          "Video_id": vi['Video ID'],
                          "Description": vi['Description'],
                          "Published at": vi['Published At'],
                          "Tags": vi['Tags'],
                          "View_count": vi['View Count'],
                          "Like_count": vi['Like Count'],
                          "Favorite_count": vi['Favorite Count'],
                          "Comment_count": vi['Comment Count'],
                          "Duration": vi['Duration'],
                          "Thumbnail": vi['Thumbnail URL'],
                          "Caption_status": vi['Caption Status'],
                          "comments": []}
            # loop over each comment on the video
            for cmt in comment_list:
                if cmt['Video ID'] == vi['Video ID']:
                    # create a nested dictionary for the comment
                    comment_dict = {
                                    "Video_id": cmt["Video ID"],
                                    "Comment ID": cmt["Comment ID"],
                                    "Author name": cmt["Author name"],
                                    "Comment": cmt["Comment"],
                                    "Published at": cmt["Published at"],
                                    }
                    # add the comment to the video
                    video_dict['comments'].append(comment_dict)
            # add the video to the playlist
            playlist_dict['videos'].append(video_dict)
    # add the playlist to the channel
    channel_dict['playlists'].append(playlist_dict)


cha = {
    'id': channel_data(ch_search)['Id'],
    'name': channel_data(ch_search)['Name of the channel'],
    'views': channel_data(ch_search)['Views'],
    'subscribers': channel_data(ch_search)['Subscribers'],
    'description': channel_data(ch_search)['Description'],
    'status': channel_data(ch_search)['Status'],
    }

# channel_dfs = pd.DataFrame.from_dict([cha]).copy()
# c_df = channel_dfs.copy()
fi_vid = fi_video.copy()
fi_vid.drop('Tags', axis=1, inplace=True)
fi_vid.drop_duplicates(subset='Video ID', keep='first', inplace=True)


# Generate name of the collection in mongodb
def generate_channel_name():
    channel_name = cha['name']
    return channel_name


# Create connection with mongodb
client = MongoClient("mongodb://localhost:27017/")
db = client["youtube"]
Channel_name = generate_channel_name()
collection = db[Channel_name]

# pla_list = play_lists.copy()
# pla_list.drop('Playlists_count', axis=1, inplace=True)
# pla_list.drop_duplicates(subset='Playlist_id', keep='first', inplace=True)
# def convert_datetime_to_string(dt):
#     return dt.strftime("%Y-%m-%d %H:%M:%S")
#
#
# def compress_data(data):
#     compressed_data = zlib.compress(json.dumps(data).encode())
#     return compressed_data
#
#
# converted_data = {
#     key: convert_datetime_to_string(value) if isinstance(value, datetime) else value
#     for key, value in channel_dict.items()
# }
# compressed = compress_data(converted_data)

# Create a button to upload data retrieved from youtube api
if st.button('Upload'):
    # Upload the document to MongoDB
    collection.insert_one(channel_dict)
    st.success("Data uploaded successfully!")

# Create a selectbox for data to be migrated from mongodb
channel_selected = st.selectbox("Select channel", db.list_collection_names())


# Migration to sqlite
if st.button('Migrate'):
    collection = db[channel_selected]
    # Retrieve documents from the collection
    documents = collection.find()

    # Initialize empty lists to store extracted data
    channel_dat = []
    playlist_dat = []
    video_dat = []
    comment_dat = []

    # Iterate over the documents
    for document in documents:
        # Extract channel information
        channel_info = {
            'id': document['id'],
            'name': document['name'],
            'views': document['views'],
            'subscribers': document['subscribers'],
            'description': document['description'],
            'status': document['status']
        }
        channel_dat.append(channel_info)

        # Extract playlist information
        for playlist in document['playlists']:
            playlist_info = {
                'Playlist id': playlist['Playlist id'],
                'Playlist name': playlist['Playlist name'],
                'video_count': playlist['video_count'],
                'id': document['id']
            }
            playlist_dat.append(playlist_info)

            # Extract video information
            for video in playlist['videos']:
                video_info = {
                    'Playlist id': playlist['Playlist id'],
                    'Video_id': video['Video_id'],
                    'Title': video['Title'],
                    'Description': video['Description'],
                    'Published at': video['Published at'],
                    'View Count': video['View_count'],
                    'Like Count': video['Like_count'],
                    'Favorite Count': video['Favorite_count'],
                    'Comment Count': video['Comment_count'],
                    'Duration': video['Duration'],
                    'Thumbnail': video['Thumbnail'],
                    'Caption Status': video['Caption_status']
                }
                video_dat.append(video_info)

                # Extract comment information
                for comment in video['comments']:
                    comment_info = {
                        'Video_id': comment['Video_id'],
                        'Comment ID': comment['Comment ID'],
                        'Author name': comment['Author name'],
                        'Comment': comment['Comment'],
                        'Published at': comment['Published at']
                    }
                    comment_dat.append(comment_info)

    # Create dataframes
    channel_df = pd.DataFrame(channel_dat)
    playlist_df = pd.DataFrame(playlist_dat)
    playlist_df.drop('video_count', axis=1, inplace=True)
    playlist_df.drop_duplicates(subset='Playlist id', keep='first', inplace=True)
    video_df = pd.DataFrame(video_dat)
    video_df['Published at'] = video_df['Published at'].astype(str)
    video_df.drop_duplicates(subset='Video_id', keep='first', inplace=True)
    comment_df = pd.DataFrame(comment_dat)
    comment_df['Published at'] = comment_df['Published at'].astype(str)
    comment_df.drop_duplicates(subset='Comment ID', keep='first', inplace=True)

    conn = sqlite3.connect('C:\sqlite\youtube.db')

    # Create a cursor object
    cursor = conn.cursor()

    conn.execute('BEGIN TRANSACTION')
    # Specify column mapping if needed (assuming column names in the table are 'column1' and 'column2')
    ch_column_mapping = {'id': 'id', 'name': 'channel_name', 'views': 'views', 'subscribers': 'subscribers',
                         'description': 'description', 'status': 'status'}
    sql_ch = f"INSERT INTO channel ({', '.join(ch_column_mapping.values())}) VALUES (?, ?, ?, ?, ?, ?)"

    for row in channel_df.itertuples(index=False):
        cursor.execute(sql_ch, row)

    # Specify column mapping if needed (assuming column names in the table are 'column1' and 'column2')
    pl_column_mapping = {'Playlist id': 'playlist_id', 'Playlist_name': 'playlist_name', 'id': 'id'}
    sql_pl = f"INSERT INTO playlists ({', '.join(pl_column_mapping.values())}) VALUES (?, ?, ?)"

    for row in playlist_df.itertuples(index=False):
        cursor.execute(sql_pl, row)

    # Specify column mapping if needed (assuming column names in the table are 'column1' and 'column2')
    vi_column_mapping = {'Playlist id': 'playlist_id', 'Video_id': 'video_id', 'Title': 'video_name',
                         'Description': 'description', 'Published at': 'published_date', 'View Count': 'view_count',
                         'Like Count': 'like_count', 'Favorite Count': 'favorite_count',
                         'Comment Count': 'comment_count', 'Duration': 'duration',
                         'Thumbnail': 'thumbnail', 'Caption Status': 'caption_status'}

    sql_vi = f"INSERT INTO videos ({', '.join(vi_column_mapping.values())}) " \
             f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    for row in video_df.itertuples(index=False):
        cursor.execute(sql_vi, row)

    # Specify column mapping if needed (assuming column names in the table are 'column1' and 'column2')
    cm_column_mapping = {'Video_id': 'video_id', 'Comment ID': 'comment_id', 'Author name': 'author',
                         'Comment': 'comment', 'Published at': 'published_date'}
    sql_cm = f"INSERT INTO comments ({', '.join(cm_column_mapping.values())}) VALUES (?, ?, ?, ?, ?)"

    for row in comment_df.itertuples(index=False):
        cursor.execute(sql_cm, row)

    # Close the connection
    conn.commit()

    st.success("Data uploaded successfully!")
    conn.close()


def execute_query(queri, column):
    # Connect to the SQLite database
    connect = sqlite3.connect('C:\sqlite\youtube.db')

    # Create a cursor object to execute SQL queries
    cursors = connect.cursor()

    # Execute the query
    cursors.execute(queri)

    # Fetch all the results
    results = cursors.fetchall()

    # Create a DataFrame from the results
    df_1 = pd.DataFrame(results, columns=column)

    # Close the cursor and connection
    cursors.close()
    connect.close()

    return df_1


if st.button('question 1'):
    query = """
        SELECT c.id, c.channel_name, p.playlist_id, p.playlist_name, v.video_id, v.video_name
        FROM channel c
        JOIN playlists p ON c.id = p.id
        JOIN videos v ON p.playlist_id = v.playlist_id
    """
    columns = ['Channel ID', 'Channel Name', 'Playlist ID', 'Playlist Name', 'Video ID', 'Video Name']
    df = execute_query(query, columns)
    st.table(df)

if st.button('question 2'):
    query = """
        SELECT c.channel_name, COUNT(*) as video_count
        FROM channel c
        JOIN playlists p ON c.id = p.id
        JOIN videos v ON p.playlist_id = v.playlist_id
        GROUP BY c.channel_name
        ORDER BY video_count DESC
    """
    columns = ['Channel Name', 'Video Count']
    df = execute_query(query, columns)
    st.table(df)

if st.button('question 3'):
    query = """
        SELECT v.video_name, c.channel_name, CAST(v.view_count AS INTEGER) AS view_count
        FROM videos v
        JOIN playlists p ON v.playlist_id = p.playlist_id
        JOIN channel c ON p.id = c.id
        ORDER BY CAST(v.view_count AS INTEGER) DESC
        LIMIT 10 
    """
    columns = ['Video Title', 'Channel Name', 'View Count']
    df = execute_query(query, columns)
    st.table(df)

if st.button('question 4'):
    query = """
        SELECT video_name, CAST(comment_count AS INTEGER) AS comments_count
        FROM videos
        ORDER BY CAST(comment_count AS INTEGER) DESC
        LIMIT 20
    """
    columns = ['Video Title', 'Comment Count']
    df = execute_query(query, columns)
    st.table(df)

if st.button('question 5'):
    query = """
        SELECT c.channel_name, CAST(v.like_count AS INTEGER) AS likes_count
        FROM videos v
        JOIN playlists p ON v.playlist_id = p.playlist_id
        JOIN channel c ON p.id = c.id
        ORDER BY CAST(like_count AS INTEGER) DESC
        LIMIT 10
    """
    columns = ['Channel name', 'Like Count']
    df = execute_query(query, columns)
    st.table(df)

if st.button('question 6'):
    query = """
        SELECT video_name, CAST(like_count AS INTEGER) AS likes_count
        FROM videos 
        ORDER BY CAST(like_count AS INTEGER) DESC
        LIMIT 10
    """
    columns = ['Video Title', 'Like Count']
    df = execute_query(query, columns)
    st.table(df)

if st.button('question 7'):
    query = """
        SELECT c.channel_name, CAST(v.view_count AS INTEGER) AS views_count
        FROM videos v
        JOIN playlists p ON v.playlist_id = p.playlist_id
        JOIN channel c ON p.id = c.id
        ORDER BY CAST(view_count AS INTEGER) DESC
        LIMIT 10
    """
    columns = ['Channel name', 'View Count']
    df = execute_query(query, columns)
    st.table(df)

if st.button('question 8'):
    query = """
        SELECT DISTINCT c.channel_name
        FROM channel c
        JOIN playlists p ON c.id = p.id
        JOIN videos v ON p.playlist_id = v.playlist_id
        WHERE v.published_date LIKE '%2022%'
    """
    columns = ['Channel name']
    df = execute_query(query, columns)
    st.table(df)

if st.button('question 9'):
    query = """
        SELECT c.channel_name, AVG(CAST(SUBSTR(v.duration, 3, INSTR(v.duration, 'M') - 3) AS INTEGER)) 
        AS average_duration
        FROM channel c
        JOIN playlists p ON c.id = p.id
        JOIN videos v ON p.playlist_id = v.playlist_id
        GROUP BY c.id
    """
    columns = ['Channel name', 'Average duration']
    df = execute_query(query, columns)
    st.table(df)

if st.button('question 10'):
    query = """
        SELECT v.video_name, c.channel_name, CAST(v.comment_count AS INTEGER) AS comment_count
        FROM videos v
        JOIN playlists p ON v.playlist_id = p.playlist_id
        JOIN channel c ON p.id = c.id
        GROUP BY v.video_name, c.channel_name
        ORDER BY comment_count DESC
        LIMIT 10
    """
    columns = ['Video title', 'Channel name', 'Comment count']
    df = execute_query(query, columns)
    st.table(df)
