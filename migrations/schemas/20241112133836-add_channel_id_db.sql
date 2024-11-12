
-- +migrate Up
ALTER TABLE messages ADD COLUMN channel_id BIGINT;
-- +migrate Down
