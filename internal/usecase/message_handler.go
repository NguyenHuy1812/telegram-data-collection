package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/gotd/td/tg"

	"github.com/NguyenHuy1812/telegram-data-collection/internal/repository"
	"github.com/NguyenHuy1812/telegram-data-collection/internal/model"
)

type MessageHandler struct {
	repo repository.MessageRepository
}

func NewMessageHandler(repo repository.MessageRepository) *MessageHandler {
	return &MessageHandler{
		repo: repo,
	}
}

func (h *MessageHandler) HandleChannelMessage(ctx context.Context, e tg.Entities, u *tg.UpdateNewChannelMessage) error {
	msg, ok := u.Message.(*tg.Message)
	if !ok {
		return nil
	}

	// Get channel info from entities
	channel, ok := e.Channels[msg.GetPeerID().(*tg.PeerChannel).ChannelID]
	if !ok {
		return fmt.Errorf("channel not found in entities")
	}

	// Create a new message model with enhanced data
	message := &model.Message{
		ChatID:    msg.GetPeerID().(*tg.PeerChannel).ChannelID,
		ChannelID: msg.GetPeerID().(*tg.PeerChannel).ChannelID,
		UserID:    func() *int64 {
			if msg.FromID == nil {
				return nil
			}
			if peer, ok := msg.FromID.(*tg.PeerUser); ok {
				id := peer.UserID
				return &id
			}
			return nil
		}(),
		Text:      &msg.Message,
		CreatedAt: time.Unix(int64(msg.Date), 0),
		MessageID: int64(msg.ID),
		ChannelTitle: func() *string {
			if channel.Username != "" {
				title := channel.Title
				return &title
			}
			return nil
		}(),
		ChannelUsername: &channel.Username,
		ChannelMemberCount: func() *int32 {
			count := int32(channel.ParticipantsCount)
			return &count
		}(),
		Views:    func() *int32 {
			if msg.Views != 0 {
				views := int32(msg.Views)
				return &views
			}
			return nil
		}(),
		Forwards: func() *int32 {
			if msg.Forwards != 0 {
				forwards := int32(msg.Forwards)
				return &forwards
			}
			return nil
		}(),
		Replies:  func() *int32 {
			if &msg.Replies == nil {
				return nil
			}
			replies := int32(msg.Replies.Replies)
			return &replies
		}(),
		HasMedia: &msg.Media != nil,
		MediaType: func() *string {
			if msg.Media == nil {
				return nil
			}
			mediaType := getMediaType(msg.Media)
			return &mediaType
		}(),
	}

	return h.repo.SaveMessage(ctx, message)
}

// Add helper function to determine media type
func getMediaType(media tg.MessageMediaClass) string {
	switch media.(type) {
	case *tg.MessageMediaPhoto:
		return "photo"
	case *tg.MessageMediaDocument:
		return "document"
	case *tg.MessageMediaGeo:
		return "geo"
	case *tg.MessageMediaContact:
		return "contact"
	case *tg.MessageMediaPoll:
		return "poll"
	case *tg.MessageMediaVenue:
		return "venue"
	case *tg.MessageMediaWebPage:
		return "webpage"
	default:
		return "other"
	}
} 