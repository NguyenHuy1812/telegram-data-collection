-- +migrate Up
ALTER TABLE messages
ADD COLUMN message_id INTEGER,
ADD COLUMN channel_title VARCHAR(255),
ADD COLUMN channel_username VARCHAR(255),
ADD COLUMN channel_member_count INTEGER,
ADD COLUMN views INTEGER,
ADD COLUMN forwards INTEGER,
ADD COLUMN replies INTEGER,
ADD COLUMN has_media BOOLEAN,
ADD COLUMN media_type VARCHAR(50);

-- Create indexes for common queries
CREATE INDEX idx_messages_message_id ON messages(message_id);
CREATE INDEX idx_messages_channel_username ON messages(channel_username);

-- +migrate Down
ALTER TABLE messages
DROP COLUMN message_id,
DROP COLUMN channel_title,
DROP COLUMN channel_username,
DROP COLUMN channel_member_count,
DROP COLUMN views,
DROP COLUMN forwards,
DROP COLUMN replies,
DROP COLUMN has_media,
DROP COLUMN media_type;

DROP INDEX IF EXISTS idx_messages_message_id;
DROP INDEX IF EXISTS idx_messages_channel_username; 