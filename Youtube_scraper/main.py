import streamlit as st
import requests_cache
import sqlite3
from googleapiclient.discovery import build
from pymongo import MongoClient
import pandas as pd
import datetime
# import zlib
# import json

st.set_page_config(layout='wide')
requests_cache.install_cache('youtube_cache', expire_after=3600)
api_key = "AIzaSyAp5tsDKpHezXQP0BfwzqM1YINVARtPS0U"
youtube = build('youtube', 'v3', developerKey=api_key)
client = MongoClient("mongodb://localhost:27017/")
db = client["youtube"]

c1, c2 = st.columns([6, 6])

with c2:
    c_id = st.text_input("Enter the channel id:", "UCGBnz-FR3qaowYsyIEh2-zw")

ch_search = youtube.channels().list(
            id=c_id, part='snippet,statistics,status').execute()
# vi_search = youtube.search().list(
#             channelId=c_id, part="snippet,id", maxResults=50, order="date", pageToken=next_page_token).execute()


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


play_lists = pd.DataFrame(playlist_data(c_id)).copy()
play_list = play_lists.to_dict('records').copy()
pl = play_lists["Playlist_id"].tolist().copy()


# define function to get videos in a playlist
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


# define function to get videos in multiple playlists
def get_videos_in_playlists(playlists_ids):
    vid = []
    for playlist_id in playlists_ids:
        playlist_videos = get_playlist_videos(playlist_id)
        vid.extend(playlist_videos)
    return vid


pl_lists = pd.DataFrame(get_videos_in_playlists(pl)).copy()
pl_videos = pl_lists["Video ID"].tolist().copy()


# Define a function to retrieve video details for a list of video IDs
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


df = pd.DataFrame(video_data(pl_videos))
df_videos = df.copy()
print(len(df_videos))
original_video_ids = set(pl_lists["Video ID"])
retrieved_video_ids = set(df["Video ID"])
missing_id = original_video_ids - retrieved_video_ids
missing = pl_lists[pl_lists["Video ID"].isin(missing_id)]
pl_lists.drop(missing.index, inplace=True)


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
c3, c4 = st.columns([4, 12])

header_color = "#FF0000"
with c3:
    for key, value in channel_data(ch_search).items():
        st.markdown(f"<h5 style='color:{header_color}'>{key}</h5>", unsafe_allow_html=True)
        st.write(value)

with c4:
    st.write(pd.DataFrame(playlist_data(c_id)))
    st.write(pd.DataFrame(get_videos_in_playlists(pl)))
    st.write(pd.DataFrame(video_data(pl_videos)))

comment_list = comments_data()
pi_video = pl_lists['playlist_id']
fi_video = df.copy()
position = 0
fi_video.insert(position, 'playlist_id', pi_video)
fi_video['Published At'] = fi_video['Published At'].dt.strftime("%Y-%m-%d %H:%M:%S")
f_video = fi_video.to_dict('records').copy()
print(len(fi_video))
print(len(pl_lists))

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

channel_dfs = pd.DataFrame.from_dict([cha]).copy()
channel_df = channel_dfs.copy()
fi_vid = fi_video.copy()
fi_vid.drop('Tags', axis=1, inplace=True)
fi_vid.drop_duplicates(subset='Video ID', keep='first', inplace=True)
print(fi_vid['Title'].nunique())


def generate_channel_name():
    collection_count = len(db.list_collection_names())  # Count existing documents and add 1
    return f"Channel {collection_count + 1}"


Channel_name = generate_channel_name()
collection = db[Channel_name]

pla_list = play_lists.copy()
pla_list.drop('Playlists_count', axis=1, inplace=True)
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

if st.button('Upload'):
    # Upload the document to MongoDB
    collection.insert_one(channel_dict)
    st.success("Data uploaded successfully!")


# Migration
if st.button('Migrate'):
    conn = sqlite3.connect('C:\sqlite\youtube.db')

    # Create a cursor object
    cursor = conn.cursor()

    try:
        conn.execute('BEGIN TRANSACTION')
        # Specify column mapping if needed (assuming column names in the table are 'column1' and 'column2')
        ch_column_mapping = {'id': 'id', 'name': 'channel_name', 'views': 'views', 'subscribers': 'subscribers',
                             'description': 'description', 'status': 'status'}
        sql_ch = f"INSERT INTO channel ({', '.join(ch_column_mapping.values())}) VALUES (?, ?, ?, ?, ?, ?)"

        for row in channel_df.itertuples(index=False):
            cursor.execute(sql_ch, row)

        # Specify column mapping if needed (assuming column names in the table are 'column1' and 'column2')
        pl_column_mapping = {'Playlist_id': 'playlist_id', 'Playlist_title': 'playlist_name', 'Id': 'id'}
        sql_pl = f"INSERT INTO playlists ({', '.join(pl_column_mapping.values())}) VALUES (?, ?, ?)"

        for row in pla_list.itertuples(index=False):
            cursor.execute(sql_pl, row)

        # Specify column mapping if needed (assuming column names in the table are 'column1' and 'column2')
        vi_column_mapping = {'playlist_id': 'playlist_id', 'Video ID': 'video_id', 'Title': 'video_name',
                             'Description': 'description', 'Published At': 'published_date', 'View Count': 'view_count',
                             'Like Count': 'like_count', 'Favorite Count': 'favorite_count',
                             'Comment Count': 'comment_count', 'Duration': 'duration',
                             'Thumbnail URL': 'thumbnail', 'Caption Status': 'caption_status'}

        sql_vi = f"INSERT INTO videos ({', '.join(vi_column_mapping.values())}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

        for row in fi_vid.itertuples(index=False):
            cursor.execute(sql_vi, row)

        # Specify column mapping if needed (assuming column names in the table are 'column1' and 'column2')
        cm_column_mapping = {'Video ID': 'video_id', 'Comment ID': 'comment_id', 'Author name': 'author',
                             'Comment': 'comment', 'Published at': 'published_date'}
        sql_cm = f"INSERT INTO comments ({', '.join(cm_column_mapping.values())}) VALUES (?, ?, ?, ?, ?)"

        for row in comment_df.itertuples(index=False):
            cursor.execute(sql_cm, row)

        # Close the connection
        conn.commit()

        st.success("Data uploaded successfully!")
    except Exception as e:
        conn.rollback()  # Rollback the transaction in case of any error
        st.error(f"An error occurred: {str(e)}")

    finally:
        conn.close()