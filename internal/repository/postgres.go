package repository

import (
	"context"
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/NguyenHuy1812/telegram-data-collection/internal/model"
)

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) SaveMessage(ctx context.Context, msg *model.Message) error {
	query := `
		INSERT INTO messages (
			chat_id, channel_id, user_id, message_id, text, created_at,
			channel_title, channel_username, channel_member_count,
			views, forwards, replies, has_media, media_type
		)
		VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
		)
		RETURNING id`

	err := r.db.QueryRowContext(ctx, query,
		msg.ChatID,
		msg.ChannelID,
		msg.UserID,
		msg.MessageID,
		msg.Text,
		msg.CreatedAt,
		msg.ChannelTitle,
		msg.ChannelUsername,
		msg.ChannelMemberCount,
		msg.Views,
		msg.Forwards,
		msg.Replies,
		msg.HasMedia,
		msg.MediaType,
	).Scan(&msg.ID)

	return err
}

func (r *PostgresRepository) GetMessages(ctx context.Context, chatID int64) ([]model.Message, error) {
	query := `
		SELECT id, chat_id, channel_id, user_id, text, created_at
		FROM messages
		WHERE chat_id = $1
		ORDER BY created_at DESC`

	rows, err := r.db.QueryContext(ctx, query, chatID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []model.Message
	for rows.Next() {
		var msg model.Message
		err := rows.Scan(
			&msg.ID,
			&msg.ChatID,
			&msg.ChannelID,
			&msg.UserID,
			&msg.Text,
			&msg.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}

	return messages, rows.Err()
} 