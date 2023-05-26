import sqlite3
# Establish connection
conn = sqlite3.connect('C:\sqlite\youtube.db')

# Create a cursor object
cursor = conn.cursor()

# Execute a SQL statement
# cursor.execute('''CREATE TABLE IF NOT EXISTS channel (id TEXT PRIMARY KEY, channel_name TEXT,
# views TEXT, subscribers TEXT, description TEXT, status TEXT)''')
#
# cursor.execute('''CREATE TABLE IF NOT EXISTS playlists (playlist_id TEXT PRIMARY KEY, playlist_name TEXT,
# id TEXT, FOREIGN KEY(id) REFERENCES channel (id))''')
#
# cursor.execute('CREATE TABLE IF NOT EXISTS videos (playlist_id TEXT, video_id TEXT PRIMARY KEY, video_name TEXT, '
#                'description TEXT, published_date TEXT, view_count TEXT, like_count TEXT, '
#                'favorite_count TEXT, comment_count TEXT, duration TEXT, thumbnail TEXT, caption_status TEXT, '
#                'FOREIGN KEY(playlist_id) REFERENCES playlists(playlist_id))')

# cursor.execute('CREATE TABLE IF NOT EXISTS comments (video_id TEXT, comment_id TEXT PRIMARY KEY, author TEXT, '
#                'comment TEXT, published_date TEXT, FOREIGN KEY (video_id) REFERENCES videos(video_id))')

# Commit the changes
conn.commit()

# Close the connection
conn.close()