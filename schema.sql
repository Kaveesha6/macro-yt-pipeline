CREATE TABLE IF NOT EXISTS youtube_video_performance (
    id                  SERIAL PRIMARY KEY,
    video_id            VARCHAR(20) NOT NULL,
    video_title         TEXT,
    channel_id          VARCHAR(50),
    channel_name        VARCHAR(255),
    playlist_id         VARCHAR(100),
    playlist_name       VARCHAR(255),
    publish_datetime    TIMESTAMPTZ,
    video_duration      VARCHAR(20),
    view_count          BIGINT,
    like_count          BIGINT,
    comment_count       BIGINT,
    data_capture_date   DATE,
    data_capture_ts_utc TIMESTAMPTZ,
    UNIQUE(video_id, data_capture_date)
);