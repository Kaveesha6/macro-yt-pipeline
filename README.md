# YouTube Performance Pipeline — Macro Labs

## Overview
A daily ELT pipeline that fetches YouTube video performance data 
from the YouTube Data API v3 and stores it in a PostgreSQL database. 
A Looker Studio dashboard provides insights across last 24 hours, 
7 days, and 28 days.

## Requirements
- Python 3.8+
- PostgreSQL or Supabase account
- YouTube Data API v3 key

## Setup Instructions

### 1. Clone the repository
git clone https://github.com/YOUR_USERNAME/macro-yt-pipeline.git
cd macro-yt-pipeline

### 2. Install dependencies
pip install google-api-python-client psycopg2-binary python-dotenv

### 3. Create .env file
Create a file called .env in the project folder with:

YOUTUBE_API_KEY=your_youtube_api_key
CHANNEL_IDS=UCxxxxxxxx,UCyyyyyyyy
DB_HOST=your_database_host
DB_PORT=5432
DB_NAME=postgres
DB_USER=your_db_user
DB_PASSWORD=your_db_password

### 4. Create the database table
Run schema.sql on your PostgreSQL database using pgAdmin 
or psql:
psql -U postgres -d youtube_pipeline -f schema.sql

### 5. Run the pipeline
python pipeline.py

### 6. Automate daily runs
Windows: Use Task Scheduler to run pipeline.py daily at 6:00 AM

## Channels Tracked
- Hiru TV
- Sirasa TV
- ITN Sri Lanka
- TV Derana

## Dashboard
Built with Looker Studio connected to Supabase PostgreSQL.
Shows performance across Last 24 Hours, Last 7 Days, Last 28 Days.

## Assumptions and Limitations
- Playlist fields are null — YouTube API v3 does not directly 
  link videos to playlists from the uploads endpoint
- Pipeline fetches last 28 days of videos on each run
- Performance metrics reflect state at time of data capture
- Daily scheduling via Windows Task Scheduler
- YouTube API free quota: 10,000 units/day
- Time windows based on publish_datetime in UTC
