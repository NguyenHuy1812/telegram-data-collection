package usecase

import (
	"context"
	"fmt"

	"github.com/gotd/contrib/storage"
	"github.com/gotd/td/tg"
)

type MessageHandler struct {
	peerDB storage.PeerStorage
}

func NewMessageHandler(peerDB storage.PeerStorage) *MessageHandler {
	return &MessageHandler{
		peerDB: peerDB,
	}
}

func (h *MessageHandler) HandleChannelMessage(ctx context.Context, e tg.Entities, u *tg.UpdateNewChannelMessage) error {
	msg, ok := u.Message.(*tg.Message)
	if !ok {
		return nil
	}

	p, err := storage.FindPeer(ctx, h.peerDB, msg.GetPeerID())
	if err != nil {
		return nil
	}
	fmt.Printf("channelID: %d - %s\nmsg: %s\n", p.Channel.ID, p.Channel.Title, msg.Message)

	return nil
} 