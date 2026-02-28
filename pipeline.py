# =============================================================
# YouTube Daily Performance Pipeline
# Purpose: Fetch video data from YouTube API and store in 
#          PostgreSQL database for daily performance reporting
# Author: Kaveesha Gimhani | Macro Labs Assessment
# =============================================================

# --- Import required libraries ---
import os                          # Used to read environment variables from .env file
import re                          # Used for regular expressions (to parse video duration)
from datetime import datetime, timezone, timedelta  # Used for date/time calculations
from dotenv import load_dotenv     # Used to load .env file into environment variables
from googleapiclient.discovery import build  # Used to connect to YouTube Data API v3
import psycopg2                    # Used to connect to PostgreSQL database

# Load all variables from the .env file into the environment
# This reads YOUTUBE_API_KEY, CHANNEL_IDS, DB_HOST, etc.
load_dotenv()

# --- Configuration: Read all settings from .env file ---
# Get the YouTube API key from .env
API_KEY = os.getenv("YOUTUBE_API_KEY")

# Get the list of channel IDs from .env and split by comma
# e.g. "UCaaa,UCbbb" becomes ["UCaaa", "UCbbb"]
CHANNEL_IDS = os.getenv("CHANNEL_IDS", "").split(",")

# Build the database connection configuration dictionary
# These values are all read from the .env file
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),   # Database server address
    "port":     os.getenv("DB_PORT", 5432),          # Database port (default 5432)
    "dbname":   os.getenv("DB_NAME", "youtube_pipeline"),  # Database name
    "user":     os.getenv("DB_USER", "postgres"),    # Database username
    "password": os.getenv("DB_PASSWORD", ""),        # Database password
}

# Build the YouTube API client using the API key
# This creates the connection object we use to make API calls
youtube = build("youtube", "v3", developerKey=API_KEY)


# =============================================================
# FUNCTION 1: Get the uploads playlist ID for a channel
# Every YouTube channel has a special hidden playlist that 
# contains all its uploaded videos. We need this playlist ID
# to fetch the list of videos from a channel.
# =============================================================
def get_upload_playlist_id(channel_id):
    # Call the YouTube API to get channel details
    # "contentDetails" gives us playlist info
    # "snippet" gives us the channel name
    res = youtube.channels().list(
        part="contentDetails,snippet",
        id=channel_id
    ).execute()

    # Get the list of items returned by the API
    items = res.get("items", [])

    # If no items found, the channel ID is wrong — return None
    if not items:
        return None, None

    # Extract the channel name from the API response
    channel_name = items[0]["snippet"]["title"]

    # Extract the uploads playlist ID from the API response
    # This is the special playlist that holds all uploaded videos
    upload_pl_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

    # Return both the uploads playlist ID and the channel name
    return upload_pl_id, channel_name


# =============================================================
# FUNCTION 2: Get video IDs from the uploads playlist
# This fetches all video IDs uploaded in the last 28 days.
# The playlist is ordered newest first, so we stop when we
# reach a video older than our cutoff date.
# =============================================================
def get_recent_video_ids(upload_playlist_id, days=28):
    # Calculate the cutoff date (28 days ago from now in UTC)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    # List to store the video IDs we find
    video_ids = []

    # Variable to handle pagination (YouTube returns max 50 items per request)
    next_page = None

    # Keep fetching pages of results until there are no more pages
    while True:
        # Call the YouTube API to get playlist items (videos in the uploads playlist)
        res = youtube.playlistItems().list(
            part="snippet",              # We need snippet to get publish date and video ID
            playlistId=upload_playlist_id,  # The uploads playlist of the channel
            maxResults=50,               # Maximum allowed per request
            pageToken=next_page          # Used for pagination (None on first request)
        ).execute()

        # Loop through each video item in the response
        for item in res.get("items", []):
            # Get the publish date/time of this video
            published = item["snippet"]["publishedAt"]

            # Convert the publish date string to a datetime object with timezone
            pub_dt = datetime.fromisoformat(published.replace("Z", "+00:00"))

            # Check if this video was published within our cutoff window
            if pub_dt >= cutoff:
                # If yes, add the video ID to our list
                video_ids.append(item["snippet"]["resourceId"]["videoId"])
            else:
                # If the video is older than cutoff, stop fetching
                # (playlist is ordered newest first, so all remaining are older)
                return video_ids

        # Check if there is another page of results
        next_page = res.get("nextPageToken")

        # If no more pages, exit the loop
        if not next_page:
            break

    # Return the complete list of video IDs
    return video_ids


# =============================================================
# FUNCTION 3: Get detailed stats for a list of video IDs
# YouTube API allows fetching details for up to 50 videos
# at once, so we process videos in batches of 50.
# =============================================================
def get_video_details(video_ids):
    # List to store the results
    results = []

    # Process video IDs in batches of 50 (API limit)
    for i in range(0, len(video_ids), 50):
        # Take a slice of 50 video IDs
        batch = video_ids[i:i+50]

        # Call the YouTube API to get video details
        # "snippet" = title, channel, publish date
        # "statistics" = views, likes, comments
        # "contentDetails" = duration
        res = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(batch)   # Join video IDs with comma as required by API
        ).execute()

        # Add the results from this batch to our main results list
        results.extend(res.get("items", []))

    # Return all video details
    return results


# =============================================================
# FUNCTION 4: Get playlist info for a video (best effort)
# Note: This function exists but is not called in the main run
# because fetching playlist per video uses too much API quota.
# Playlist fields are stored as NULL in the database.
# =============================================================
def get_playlist_for_video(video_id):
    """Search playlists containing this video - returns first match."""
    try:
        # Search YouTube for playlists related to this video ID
        res = youtube.search().list(
            part="snippet",
            type="playlist",
            q=video_id,
            maxResults=1
        ).execute()

        items = res.get("items", [])

        # If a playlist is found, return its ID and name
        if items:
            pl = items[0]
            return pl["id"]["playlistId"], pl["snippet"]["title"]
    except Exception:
        # If any error occurs, silently return None
        pass

    # Return None if no playlist found or error occurred
    return None, None


# =============================================================
# FUNCTION 5: Parse ISO 8601 duration to readable HH:MM:SS
# YouTube API returns duration in ISO 8601 format like "PT4M13S"
# This function converts it to readable format like "00:04:13"
# =============================================================
def parse_duration(iso_duration):
    # Use regex to extract hours, minutes, and seconds from the ISO duration string
    # Example: "PT1H4M13S" → hours=1, minutes=4, seconds=13
    match = re.match(
        r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', iso_duration
    )

    # If the format doesn't match, return the original string unchanged
    if not match:
        return iso_duration

    # Extract hours, minutes, seconds (default to 0 if not present)
    h, m, s = match.group(1), match.group(2), match.group(3)
    h, m, s = int(h or 0), int(m or 0), int(s or 0)

    # Return formatted as HH:MM:SS (e.g. "00:04:13")
    return f"{h:02d}:{m:02d}:{s:02d}"


# =============================================================
# FUNCTION 6: Insert or update a video record in the database
# Uses "upsert" — if the record already exists (same video_id 
# and date), it updates the stats. If new, it inserts it.
# This prevents duplicate rows when the pipeline runs daily.
# =============================================================
def upsert_record(cur, record):
    # Execute the SQL INSERT statement with ON CONFLICT handling
    cur.execute("""
        INSERT INTO youtube_video_performance (
            video_id, video_title, channel_id, channel_name,
            playlist_id, playlist_name, publish_datetime,
            video_duration, view_count, like_count, comment_count,
            data_capture_date, data_capture_ts_utc
        ) VALUES (
            %(video_id)s, %(video_title)s, %(channel_id)s, %(channel_name)s,
            %(playlist_id)s, %(playlist_name)s, %(publish_datetime)s,
            %(video_duration)s, %(view_count)s, %(like_count)s, %(comment_count)s,
            %(data_capture_date)s, %(data_capture_ts_utc)s
        )
        ON CONFLICT (video_id, data_capture_date)
        DO UPDATE SET
            -- If record already exists for this video on this date,
            -- update only the performance metrics (not the video metadata)
            view_count          = EXCLUDED.view_count,
            like_count          = EXCLUDED.like_count,
            comment_count       = EXCLUDED.comment_count,
            data_capture_ts_utc = EXCLUDED.data_capture_ts_utc
    """, record)  # "record" is a dictionary with all field values


# =============================================================
# MAIN FUNCTION: Orchestrates the entire pipeline
# This is the main entry point that runs everything in order:
# 1. Connect to database
# 2. For each channel: fetch videos and store in database
# 3. Commit and close the database connection
# =============================================================
def run_pipeline():
    # Get the current date and time in UTC
    now_utc = datetime.now(timezone.utc)

    # Get just the date portion (used as data_capture_date)
    today = now_utc.date()

    # Open a connection to the PostgreSQL database using DB_CONFIG settings
    conn = psycopg2.connect(**DB_CONFIG)

    # Create a cursor — used to execute SQL commands
    cur = conn.cursor()

    # Loop through each YouTube channel ID from the .env file
    for channel_id in CHANNEL_IDS:
        # Remove any accidental spaces from the channel ID
        channel_id = channel_id.strip()

        print(f"\n Processing channel: {channel_id}")

        # Step 1: Get the uploads playlist ID and channel name
        upload_pl_id, channel_name = get_upload_playlist_id(channel_id)

        # If channel not found, skip to next channel
        if not upload_pl_id:
            print(f"Could not find channel. Check channel ID.")
            continue

        print(f"  Channel name: {channel_name}")

        # Step 2: Get all video IDs published in the last 28 days
        video_ids = get_recent_video_ids(upload_pl_id, days=28)
        print(f"  Found {len(video_ids)} videos in last 28 days")

        # If no videos found, skip to next channel
        if not video_ids:
            continue

        # Step 3: Get full details (title, stats, duration) for all videos
        videos = get_video_details(video_ids)

        # Step 4: Loop through each video and save to database
        for video in videos:
            # Extract the video ID
            vid_id = video["id"]

            # Extract the different parts of the API response
            snippet = video["snippet"]          # Contains title, channel, publish date
            stats   = video.get("statistics", {})  # Contains views, likes, comments
            details = video.get("contentDetails", {})  # Contains duration

            # Playlist info is set to None (not fetched to save API quota)
            # The assignment allows playlist fields to be empty
            playlist_id, playlist_name = None, None

            # Build a dictionary with all the data for this video
            record = {
                "video_id":            vid_id,                          # Unique YouTube video ID
                "video_title":         snippet.get("title"),            # Video title
                "channel_id":          channel_id,                      # YouTube channel ID
                "channel_name":        channel_name,                    # Channel display name
                "playlist_id":         playlist_id,                     # NULL (not fetched)
                "playlist_name":       playlist_name,                   # NULL (not fetched)
                "publish_datetime":    snippet.get("publishedAt"),      # When video was published
                "video_duration":      parse_duration(details.get("duration", "")),  # HH:MM:SS format
                "view_count":          int(stats.get("viewCount",    0)),  # Total views
                "like_count":          int(stats.get("likeCount",    0)),  # Total likes
                "comment_count":       int(stats.get("commentCount", 0)),  # Total comments
                "data_capture_date":   today,                           # Today's date
                "data_capture_ts_utc": now_utc,                        # Exact timestamp in UTC
            }

            # Save (insert or update) this record in the database
            upsert_record(cur, record)

            # Print confirmation showing first 60 characters of the video title
            print(f"  ✅ {snippet.get('title', vid_id)[:60]}")

    # Commit all changes to the database (saves everything permanently)
    conn.commit()

    # Close the cursor and database connection to free up resources
    cur.close()
    conn.close()

    print("\n Pipeline complete!")


# =============================================================
# Entry point: This runs the pipeline when you execute:
# python pipeline.py
# The "if __name__ == '__main__'" check ensures this only runs
# when the script is run directly, not when imported elsewhere.
# =============================================================
if __name__ == "__main__":
    run_pipeline()