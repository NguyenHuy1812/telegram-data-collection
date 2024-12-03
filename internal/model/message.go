package model

import "time"

type Message struct {
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
	EditDate           *int32
	PostAuthor         *string
	GroupedID          *int64
	HasReactions       bool
	ReactionCount      *int32
	TTLPeriod          *int32
	MediaPath          *string
	MediaSize          *int64
	MediaFileID        *string
	MediaMimeType      *string
}
