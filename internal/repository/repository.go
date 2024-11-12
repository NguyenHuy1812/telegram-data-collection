package repository

import (
	"context"
	"github.com/NguyenHuy1812/telegram-data-collection/internal/model"
)

type MessageRepository interface {
	SaveMessage(ctx context.Context, msg *model.Message) error
	GetMessages(ctx context.Context, chatID int64) ([]model.Message, error)
} 