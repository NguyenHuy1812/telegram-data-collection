package model

import "time"

type Message struct {
	ID                 int64
	ChatID             int64
	ChannelID          int64
	UserID             *int64
	Text               *string
	CreatedAt          time.Time
	MessageID          int64
	ChannelTitle       *string
	ChannelUsername    *string
	ChannelMemberCount *int32
	Views              *int32
	Forwards           *int32
	Replies            *int32
	HasMedia           bool
	MediaType          *string
}
