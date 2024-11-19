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
	batchSize      = 1000
	bucketName     = "dwarvesf-telegram"
	basePath       = "messages"
)

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
	}
	fmt.Println(message)
	// Get chatID
	chatID := message.ChatID

	currentDate := time.Now().Truncate(24 * time.Hour)
	needsSave := false
	isEndOfDay := false

	h.batchMutex.Lock()
	if h.batchMap[chatID] == nil {
		h.batchMap[chatID] = make([]*model.Message, 0, batchSize)
	}
	h.batchMap[chatID] = append(h.batchMap[chatID], message)
	
	// Check if batch size reached
	if len(h.batchMap[chatID]) >= batchSize {
		needsSave = true
	} else if !currentDate.Equal(h.lastSaveDate) {
		// If we've crossed into a new day
		if len(h.batchMap[chatID]) > 0 {
			needsSave = true
			isEndOfDay = true
		}
	}

	var batch []*model.Message
	if needsSave {
		batch = h.batchMap[chatID]
		h.batchMap[chatID] = make([]*model.Message, 0, batchSize) // Reset the batch
		
		// Only update lastSaveDate if this is an end-of-day save
		if isEndOfDay {
			h.lastSaveDate = currentDate
		}
	}
	h.batchMutex.Unlock()

	// Save the batch if needed
	if needsSave {
		// Use the date of the messages in the batch for the filename
		batchDate := batch[0].CreatedAt.Truncate(24 * time.Hour)
		if err := h.saveBatch(ctx, chatID, batch, batchDate); err != nil {
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
