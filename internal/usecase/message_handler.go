package usecase

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
	"bytes"
	"io"
	"mime/multipart"
	"path"
	"sync"
	"strings"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet"
	pwriter "github.com/apache/arrow/go/v13/parquet/pqarrow"
	"github.com/gotd/td/tg"
	"github.com/apache/arrow/go/v13/parquet/compress"

	"github.com/NguyenHuy1812/telegram-data-collection/internal/model"
)

const (
	batchSize      = 10000
	bucketName     = "dwarvesf-telegram"
	basePath       = "messages"
	mediaPath      = "media"
)

type MediaInfo struct {
	Type     string
	MimeType string
	Size     int64
	Raw      []byte    // Raw media data
	FileID   string    // Telegram's file ID
	FileRef  []byte    // Telegram's file reference
	Path     string    // Add this field
}

type MessageHandler struct {
	baseURL      string
	client       *http.Client
	localPath    string
	batchMap     map[int64][]*model.Message  // Map of chatID to message batch
	authToken    string
	uploadMutex  sync.Mutex
	batchMutex   sync.RWMutex               // Mutex for thread-safe batch operations
	lastSaveDate time.Time    // Track the last date we saved messages
	whitelistedChannels map[int64]struct{} // Add this field
}

func NewMessageHandler(baseURL string, localPath string, authToken string, whitelistedChannels []int64) *MessageHandler {
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		log.Printf("WARNING: Could not resolve absolute path: %v", err)
		absPath = localPath
	}
	
	if err := os.MkdirAll(absPath, 0755); err != nil {
		log.Printf("ERROR: Failed to create directory %s: %v", absPath, err)
		panic(fmt.Sprintf("failed to create local directory: %v", err))
	}
	
	log.Printf("Initialized MessageHandler with local path: %s (absolute: %s)", localPath, absPath)
	
	// Initialize whitelist map
	whitelistMap := make(map[int64]struct{})
	for _, channelID := range whitelistedChannels {
		whitelistMap[channelID] = struct{}{}
	}
	
	handler := &MessageHandler{
		baseURL:      baseURL,
		client:       &http.Client{Timeout: 30 * time.Second},
		localPath:    absPath,
		batchMap:     make(map[int64][]*model.Message),
		authToken:    authToken,
		lastSaveDate: time.Now().Truncate(24 * time.Hour), // Initialize to start of current day
		whitelistedChannels: whitelistMap,
	}
	
	return handler
}

func (h *MessageHandler) HandleChannelMessage(ctx context.Context, e tg.Entities, u *tg.UpdateNewChannelMessage) error {
	msg, ok := u.Message.(*tg.Message)
	if !ok {
		return nil
	}

	// Get channel info
	channel, ok := e.Channels[msg.GetPeerID().(*tg.PeerChannel).ChannelID]
	if !ok {
		return fmt.Errorf("channel not found in entities")
	}

	// Check if channel is whitelisted
	channelID := msg.GetPeerID().(*tg.PeerChannel).ChannelID
	if _, ok := h.whitelistedChannels[channelID]; !ok {
		// Channel not in whitelist, skip processing
		return nil
	}

	// Handle media download before creating the message model
	var mediaInfo *MediaInfo
	if msg.Media != nil {
		var err error
		mediaInfo, err = h.handleMedia(ctx, msg)
		if err != nil {
			log.Printf("WARNING: Failed to handle media for message %d: %v", msg.ID, err)
		}
	}

	// Create message model
	message := &model.Message{
		ChatID:    msg.GetPeerID().(*tg.PeerChannel).ChannelID,
		ChannelID: msg.GetPeerID().(*tg.PeerChannel).ChannelID,
		UserID: func() *int64 {
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
		Views: func() *int32 {
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
		Replies: func() *int32 {
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
		EditDate: func() *int32 {
			if msg.EditDate != 0 {
				edit := int32(msg.EditDate)
				return &edit
			}
			return nil
		}(),
		PostAuthor: func() *string {
			if msg.PostAuthor != "" {
				return &msg.PostAuthor
			}
			return nil
		}(),
		GroupedID: func() *int64 {
			if msg.GroupedID != 0 {
				id := int64(msg.GroupedID)
				return &id
			}
			return nil
		}(),
		HasReactions: msg.Reactions.Results != nil,
		ReactionCount: func() *int32 {
			if reactions := msg.Reactions.Results; reactions != nil {
				count := int32(len(reactions))
				return &count
			}
			return nil
		}(),
		TTLPeriod: func() *int32 {
			if msg.TTLPeriod != 0 {
				ttl := int32(msg.TTLPeriod)
				return &ttl
			}
			return nil
		}(),
		MediaPath: func() *string {
			if mediaInfo != nil {
				return &mediaInfo.Path
			}
			return nil
		}(),
		MediaSize: func() *int64 {
			if mediaInfo != nil {
				return &mediaInfo.Size
			}
			return nil
		}(),
	}
	// Print all message fields for debugging
	log.Printf("Message details:")
	log.Printf("ID: %d", msg.ID)
	log.Printf("Message: %s", msg.Message) 
	log.Printf("Date: %d", msg.Date)
	log.Printf("FromID: %+v", msg.FromID)
	log.Printf("PeerID: %+v", msg.GetPeerID())
	log.Printf("Views: %d", msg.Views)
	log.Printf("Forwards: %d", msg.Forwards)
	log.Printf("Replies: %+v", msg.Replies)
	log.Printf("Media: %+v", msg.Media)
	log.Printf("ReplyTo: %+v", msg.ReplyTo)
	log.Printf("Entities: %+v", msg.Entities)
	log.Printf("Edit Date: %d", msg.EditDate)
	log.Printf("Post Author: %s", msg.PostAuthor)
	log.Printf("Grouped ID: %d", msg.GroupedID)
	log.Printf("Reactions: %+v", msg.Reactions)
	log.Printf("Restriction Reason: %+v", msg.RestrictionReason)
	log.Printf("TTL Period: %d", msg.TTLPeriod)
	log.Printf("Media Path: %v", message.MediaPath)
	log.Printf("Media Size: %d", message.MediaSize)
	// Get chatID
	chatID := message.ChatID

	// Use message date instead of current time
	messageDate := message.CreatedAt.Truncate(24 * time.Hour)
	needsSave := false
	isEndOfDay := false

	h.batchMutex.Lock()
	if h.batchMap[chatID] == nil {
		h.batchMap[chatID] = make([]*model.Message, 0, batchSize)
	}

	// Check if this message is from a different day than existing messages in batch
	if len(h.batchMap[chatID]) > 0 {
		existingMsgDate := h.batchMap[chatID][0].CreatedAt.Truncate(24 * time.Hour)
		if !messageDate.Equal(existingMsgDate) {
			// Save existing batch before adding new message from different day
			needsSave = true
			isEndOfDay = true
		}
	}

	// If we need to save due to day change, do it before adding new message
	var batch []*model.Message
	if needsSave {
		batch = h.batchMap[chatID]
		h.batchMap[chatID] = make([]*model.Message, 0, batchSize) // Reset the batch
		if isEndOfDay {
			h.lastSaveDate = messageDate
		}
	}
	h.batchMutex.Unlock()

	// Save the batch if needed before adding new message
	if needsSave {
		if err := h.saveBatch(ctx, chatID, batch, batch[0].CreatedAt.Truncate(24 * time.Hour)); err != nil {
			return fmt.Errorf("failed to save batch for channel %d: %w", chatID, err)
		}
	}

	// Now add the new message to the fresh batch
	h.batchMutex.Lock()
	h.batchMap[chatID] = append(h.batchMap[chatID], message)
	
	// Check if batch size reached
	needsSave = false
	if len(h.batchMap[chatID]) >= batchSize {
		batch = h.batchMap[chatID]
		h.batchMap[chatID] = make([]*model.Message, 0, batchSize)
		needsSave = true
	}
	h.batchMutex.Unlock()

	// Save if batch size reached
	if needsSave {
		if err := h.saveBatch(ctx, chatID, batch, messageDate); err != nil {
			return fmt.Errorf("failed to save batch for channel %d: %w", chatID, err)
		}
	}

	return nil
}

func (h *MessageHandler) saveBatch(ctx context.Context, chatID int64, batch []*model.Message, batchDate time.Time) error {
	if len(batch) == 0 {
		return nil
	}

	// Create channel-specific directory
	channelDir := filepath.Join(h.localPath, fmt.Sprintf("channel_%d", chatID))
	if err := os.MkdirAll(channelDir, 0755); err != nil {
		return fmt.Errorf("failed to create channel directory: %w", err)
	}

	// Get next chunk number for this specific channel and date
	chunkNum, err := h.getNextChunkNumberForChannelAndDate(chatID, batchDate)
	if err != nil {
		log.Printf("WARNING: Error getting chunk number for channel %d: %v, using timestamp", chatID, err)
		chunkNum = time.Now().Unix()
	}

	// Generate filename using the batch date instead of current time
	filename := fmt.Sprintf("%d_%d_%02d_chunk%03d.parquet", 
		chatID,
		batchDate.Year(),
		batchDate.Month(),
		chunkNum,
	)
	
	filepath := filepath.Join(channelDir, filename)

	// Save and upload logic remains the same
	if err := h.saveToParquet(ctx, batch, filepath); err != nil {
		return fmt.Errorf("failed to save batch to parquet: %w", err)
	}

	if err := h.uploadFile(filepath); err != nil {
		log.Printf("ERROR: Failed to upload file %s: %v", filepath, err)
	}

	return nil
}

func (h *MessageHandler) saveToParquet(ctx context.Context, messages []*model.Message, filepath string) error {
	log.Printf("Saving batch of %d messages to %s", len(messages), filepath)

	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create parquet file: %w", err)
	}
	defer file.Close()

	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithCreatedBy("telegram-collector"),
	)

	arrProps := pwriter.NewArrowWriterProperties(
		pwriter.WithStoreSchema(),
	)

	w, err := pwriter.NewFileWriter(
		createMessageSchema(),
		file,
		props,
		arrProps,
	)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer w.Close()

	// Write all messages in batch
	for _, msg := range messages {
		if err := writeMessageToParquet(w, msg); err != nil {
			return fmt.Errorf("failed to write message to parquet: %w", err)
		}
	}

	log.Printf("Successfully saved batch to %s", filepath)
	return nil
}

func writeMessageToParquet(w *pwriter.FileWriter, msg *model.Message) error {
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, createMessageSchema())
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).Append(msg.ChatID)
	bldr.Field(1).(*array.Int64Builder).Append(msg.ChannelID)

	if msg.UserID != nil {
		bldr.Field(2).(*array.Int64Builder).Append(*msg.UserID)
	} else {
		bldr.Field(2).(*array.Int64Builder).AppendNull()
	}

	if msg.Text != nil {
		bldr.Field(3).(*array.StringBuilder).Append(*msg.Text)
	} else {
		bldr.Field(3).(*array.StringBuilder).AppendNull()
	}

	bldr.Field(4).(*array.TimestampBuilder).Append(arrow.Timestamp(msg.CreatedAt.UnixMicro()))
	bldr.Field(5).(*array.Int64Builder).Append(msg.MessageID)

	if msg.ChannelTitle != nil {
		bldr.Field(6).(*array.StringBuilder).Append(*msg.ChannelTitle)
	} else {
		bldr.Field(6).(*array.StringBuilder).AppendNull()
	}

	if msg.ChannelUsername != nil {
		bldr.Field(7).(*array.StringBuilder).Append(*msg.ChannelUsername)
	} else {
		bldr.Field(7).(*array.StringBuilder).AppendNull()
	}

	if msg.ChannelMemberCount != nil {
		bldr.Field(8).(*array.Int32Builder).Append(*msg.ChannelMemberCount)
	} else {
		bldr.Field(8).(*array.Int32Builder).AppendNull()
	}

	if msg.Views != nil {
		bldr.Field(9).(*array.Int32Builder).Append(*msg.Views)
	} else {
		bldr.Field(9).(*array.Int32Builder).AppendNull()
	}

	if msg.Forwards != nil {
		bldr.Field(10).(*array.Int32Builder).Append(*msg.Forwards)
	} else {
		bldr.Field(10).(*array.Int32Builder).AppendNull()
	}

	if msg.Replies != nil {
		bldr.Field(11).(*array.Int32Builder).Append(*msg.Replies)
	} else {
		bldr.Field(11).(*array.Int32Builder).AppendNull()
	}

	bldr.Field(12).(*array.BooleanBuilder).Append(msg.HasMedia)

	if msg.MediaType != nil {
		bldr.Field(13).(*array.StringBuilder).Append(*msg.MediaType)
	} else {
		bldr.Field(13).(*array.StringBuilder).AppendNull()
	}

	if msg.EditDate != nil {
		bldr.Field(14).(*array.Int32Builder).Append(*msg.EditDate)
	} else {
		bldr.Field(14).(*array.Int32Builder).AppendNull()
	}

	if msg.PostAuthor != nil {
		bldr.Field(15).(*array.StringBuilder).Append(*msg.PostAuthor)
	} else {
		bldr.Field(15).(*array.StringBuilder).AppendNull()
	}

	if msg.GroupedID != nil {
		bldr.Field(16).(*array.Int64Builder).Append(*msg.GroupedID)
	} else {
		bldr.Field(16).(*array.Int64Builder).AppendNull()
	}

	bldr.Field(17).(*array.BooleanBuilder).Append(msg.HasReactions)

	if msg.ReactionCount != nil {
		bldr.Field(18).(*array.Int32Builder).Append(*msg.ReactionCount)
	} else {
		bldr.Field(18).(*array.Int32Builder).AppendNull()
	}

	if msg.TTLPeriod != nil {
		bldr.Field(19).(*array.Int32Builder).Append(*msg.TTLPeriod)
	} else {
		bldr.Field(19).(*array.Int32Builder).AppendNull()
	}

	if msg.MediaPath != nil {
		bldr.Field(20).(*array.StringBuilder).Append(*msg.MediaPath)
	} else {
		bldr.Field(20).(*array.StringBuilder).AppendNull()
	}

	if msg.MediaSize != nil {
		bldr.Field(21).(*array.Int64Builder).Append(*msg.MediaSize)
	} else {
		bldr.Field(21).(*array.Int64Builder).AppendNull()
	}

	record := bldr.NewRecord()
	defer record.Release()

	if err := w.Write(record); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	return nil
}

func createMessageSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "ChatID", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "ChannelID", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "UserID", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "Text", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "CreatedAt", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
			{Name: "MessageID", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "ChannelTitle", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "ChannelUsername", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "ChannelMemberCount", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "Views", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "Forwards", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "Replies", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "HasMedia", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
			{Name: "MediaType", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "EditDate", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "PostAuthor", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "GroupedID", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "HasReactions", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
			{Name: "ReactionCount", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "TTLPeriod", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "MediaPath", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "MediaSize", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		},
		nil, // metadata
	)
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

func (h *MessageHandler) uploadFile(filepath string) error {
	// Validate file exists
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", err)
	}

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filepath, err)
	}
	defer file.Close()

	// Get just the filename without the channel_ prefix
	filename := path.Base(filepath)
	// If the file is in a channel_xxx directory, we want to keep the file name only
	if strings.Contains(filepath, "channel_") {
		parts := strings.Split(filepath, string(os.PathSeparator))
		for i, part := range parts {
			if strings.HasPrefix(part, "channel_") {
				if i+1 < len(parts) {
					filename = parts[i+1] // Get the actual filename after channel_xxx directory
				}
				break
			}
		}
	}

	// Create the cloud path by combining basePath with the filename
	cloudPath := path.Join(basePath, filename)

	part, err := writer.CreateFormFile("files", filename)
	if err != nil {
		return fmt.Errorf("failed to create form file: %w", err)
	}

	if _, err := io.Copy(part, file); err != nil {
		return fmt.Errorf("failed to copy file content: %w", err)
	}

	// Add form fields
	formFields := map[string]string{
		"bucket":    bucketName,
		"base_path": basePath, // Use the base path directly
	}

	for key, value := range formFields {
		if err := writer.WriteField(key, value); err != nil {
			return fmt.Errorf("failed to add field %s: %w", key, err)
		}
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close multipart writer: %w", err)
	}

	// Create and send request
	req, err := http.NewRequest("POST", h.baseURL, body)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+h.authToken)
	
	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	log.Printf("Successfully uploaded file: %s to %s", filename, cloudPath)
	return nil
}

// Add new helper function to get chunk number for specific date
func (h *MessageHandler) getNextChunkNumberForChannelAndDate(chatID int64, date time.Time) (int64, error) {
	channelDir := filepath.Join(h.localPath, fmt.Sprintf("channel_%d", chatID))
	
	if _, err := os.Stat(channelDir); os.IsNotExist(err) {
		return 1, nil
	}

	datePrefix := fmt.Sprintf("%d_%d_%02d", chatID, date.Year(), date.Month())
	files, err := filepath.Glob(filepath.Join(channelDir, datePrefix + "_chunk*.parquet"))
	if err != nil {
		return 1, fmt.Errorf("failed to list parquet files: %w", err)
	}

	if len(files) == 0 {
		return 1, nil
	}

	maxChunk := int64(0)
	for _, file := range files {
		base := filepath.Base(file)
		if parts := strings.Split(base, "_"); len(parts) >= 4 {
			chunkPart := parts[len(parts)-1]
			chunkStr := strings.TrimPrefix(strings.TrimSuffix(chunkPart, ".parquet"), "chunk")
			if chunk, err := strconv.ParseInt(chunkStr, 10, 64); err == nil {
				if chunk > maxChunk {
					maxChunk = chunk
				}
			}
		}
	}

	return maxChunk + 1, nil
}

func (h *MessageHandler) handleMedia(ctx context.Context, msg *tg.Message) (*MediaInfo, error) {
	switch media := msg.Media.(type) {
	case *tg.MessageMediaPhoto:
		return h.handlePhotoMetadata(media)
	case *tg.MessageMediaDocument:
		return h.handleDocumentMetadata(media)
	default:
		return nil, fmt.Errorf("unsupported media type: %T", media)
	}
}

func (h *MessageHandler) handlePhotoMetadata(photo *tg.MessageMediaPhoto) (*MediaInfo, error) {
	p, ok := photo.Photo.(*tg.Photo)
	if !ok {
		return nil, fmt.Errorf("invalid photo type")
	}

	// Get the largest photo size
	var largest *tg.PhotoSize
	for _, size := range p.Sizes {
		if photoSize, ok := size.(*tg.PhotoSize); ok {
			if largest == nil || photoSize.Size > largest.Size {
				largest = photoSize
			}
		}
	}

	if largest == nil {
		return nil, fmt.Errorf("no valid photo size found")
	}

	return &MediaInfo{
		Type:     "photo",
		MimeType: "image/jpeg", // Photos are typically JPEG
		Size:     int64(largest.Size),
		FileID:   strconv.FormatInt(p.ID, 10), // Convert int64 to string
		FileRef:  p.FileReference,
		Path:     fmt.Sprintf("photos/%d.jpg", p.ID), // Add a path
	}, nil
}

func (h *MessageHandler) handleDocumentMetadata(doc *tg.MessageMediaDocument) (*MediaInfo, error) {
	document, ok := doc.Document.(*tg.Document)
	if !ok {
		return nil, fmt.Errorf("invalid document type")
	}

	// Get original filename if available
	filename := ""
	for _, attr := range document.Attributes {
		if fileAttr, ok := attr.(*tg.DocumentAttributeFilename); ok {
			filename = fileAttr.FileName
			break
		}
	}

	// Use filename to construct the path
	path := fmt.Sprintf("documents/%d_%s", document.ID, filename)
	if filename == "" {
		path = fmt.Sprintf("documents/%d", document.ID)
	}

	return &MediaInfo{
		Type:     "document",
		MimeType: getMimeType(document),
		Size:     document.Size,
		FileID:   strconv.FormatInt(document.ID, 10), // Convert int64 to string
		FileRef:  document.FileReference,
		Path:     path,
	}, nil
}

// Helper function to get MIME type from document
func getMimeType(doc *tg.Document) string {
	if doc.MimeType != "" {
		return doc.MimeType
	}
	return "application/octet-stream"
}

// Add these functions to implement actual download logic
func (h *MessageHandler) downloadPhoto(ctx context.Context, photo *tg.MessageMediaPhoto, size *tg.PhotoSize) ([]byte, error) {
	// TODO: Implement photo download using your Telegram client
	// This would involve:
	// 1. Getting the input file location
	// 2. Using getFile API to download the content
	// 3. Returning the raw bytes
	return nil, fmt.Errorf("photo download not implemented")
}

func (h *MessageHandler) downloadDocument(ctx context.Context, doc *tg.Document) ([]byte, error) {
	// TODO: Implement document download using your Telegram client
	// This would involve:
	// 1. Getting the input file location
	// 2. Using getFile API to download the content
	// 3. Returning the raw bytes
	return nil, fmt.Errorf("document download not implemented")
}
