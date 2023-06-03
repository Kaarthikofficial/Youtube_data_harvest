# Youtube_data_harvest
Harvest youtube channels data from youtube API and store it in MongoDB and MySQL for analysis

## Youtube API credential
It starts with getting API credentials of youtube in google developer console with the help google account. One can create 
multiple projects for generating new API key each time as it is limited to 10000 quotas per day

## Getting data
First, I create a variable to call the youtube data using API key. Then with the help list function in each class like channels, playlists, videos and comments I tried to get the required data from it.
### Channel data
With the help of channel id(youtube channel), I collect the data like channel name, views, description, status and subscriber count of the channel. Then append this to a dictionary created called channel_dic
### Playlist data
As like previous step, collects the data from playlist class like playlist id, title, playlists_count and stored it in a dataframe.
### Videos data
I created two functions to retrieve data related videos. In that, first one is to get the playlist and video ids. After that, used the function to iterate over the list of playlist ids collected from playlist data. Final function is created to collect the required items from the video list using pagination. The collected data are video title, description, published time, view count, like count, favorite count, comment count, duration, tags, thumbnail, caption status and append it to an empty list and convert it to a dataframe.
### Comments data
Similar to the previous steps, data related to comments like video id, comment id, author name, comment, published time are retrieved.

## Uploading to MongoDB
Once all the data are collected, it will stored in a dictionary called channel dict and all the corresponding items related to one channel will be added. This will be upload with the name of the channel in MongoDB.

## Migration to sqlite
After this, each data will be migrated to corresponding tables like channel, playlists, videos and comments. This can be further used for analysis and data visualization

## Querying the data for analysis
The following questions were queried from sqlite and displayed as result using streamlit.
1. What are the names of all the videos and their corresponding channels?
2. Which channels have the most number of videos, and how many videos do they have?
3. What are the top 10 most viewed videos and their respective channels?
4. How many comments were made on each video, and what are their corresponding video names?
5. Which videos have the highest number of likes, and what are their corresponding channel names?
6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?
7. What is the total number of views for each channel, and what are their corresponding channel names?
8. What are the names of all the channels that have published videos in the year 2022?
9. What is the average duration of all videos in each channel, and what are their corresponding channel names?
10. Which videos have the highest number of comments, and what are their corresponding channel names?




