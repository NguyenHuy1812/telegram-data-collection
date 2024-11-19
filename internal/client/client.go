package client

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"go.etcd.io/bbolt"
	"golang.org/x/time/rate"

	boltstor "github.com/gotd/contrib/bbolt"
	"github.com/gotd/contrib/middleware/floodwait"
	"github.com/gotd/contrib/middleware/ratelimit"

	"github.com/NguyenHuy1812/telegram-data-collection/internal/usecase"
	"github.com/consolelabs/mochi-toolkit/config"
	"github.com/gotd/td/examples"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/message/peer"
	"github.com/gotd/td/telegram/updates"
	"github.com/gotd/td/tg"
)

// Add this function to resolve channel usernames to IDs
func resolveChannelID(ctx context.Context, api *tg.Client, username string) (int64, error) {
	// Remove @ prefix if present
	username = strings.TrimPrefix(username, "@")
	
	// Resolve the username to get channel info
	peer, err := api.ContactsResolveUsername(ctx, username)
	if err != nil {
		return 0, fmt.Errorf("failed to resolve username @%s: %w", username, err)
	}

	// Extract channel ID from the peer
	channel, ok := peer.Chats[0].(*tg.Channel)
	if !ok {
		return 0, fmt.Errorf("@%s is not a channel", username)
	}

	return channel.ID, nil
}

// Run starts the Telegram client with the provided repository
func Run(ctx context.Context) error {
	var arg struct {
		FillPeerStorage bool
	}
	flag.BoolVar(&arg.FillPeerStorage, "fill-peer-storage", false, "fill peer storage")
	flag.Parse()
	cfg, err := config.Read()
	if err != nil {
		log.Fatal("fail to read config")
	}

	phone := cfg.GetString("TELEGRAM_PHONE")
	appID, err := strconv.Atoi(cfg.GetString("TELEGRAM_APP_ID"))
	if err != nil {
		return errors.Wrap(err, " parse app id")
	}
	appHash := cfg.GetString("TELEGRAM_APP_HASH")

	sessionDir := filepath.Join("priv/session", sessionFolder(phone))
	if err := os.MkdirAll(sessionDir, 0700); err != nil {
		return err
	}
	logFilePath := filepath.Join(sessionDir, "log.jsonl")

	fmt.Printf("Storing session in %s, logs in %s\n", sessionDir, logFilePath)

	sessionStorage := &telegram.FileSessionStorage{
		Path: filepath.Join(sessionDir, "session.json"),
	}

	dispatcher := tg.NewUpdateDispatcher()

	boltdb, err := bbolt.Open(filepath.Join(sessionDir, "updates.bolt.db"), 0666, nil)
	if err != nil {
		return errors.Wrap(err, "create bolt storage")
	}
	updatesRecovery := updates.New(updates.Config{
		Handler: dispatcher,
		Logger:  nil,
		Storage: boltstor.NewStateStorage(boltdb),
	})

	waiter := floodwait.NewWaiter().WithCallback(func(ctx context.Context, wait floodwait.FloodWait) {
		fmt.Println("Got FLOOD_WAIT. Will retry after", wait.Duration)
	})

	options := telegram.Options{
		Logger:         nil,
		SessionStorage: sessionStorage,
		UpdateHandler:  updatesRecovery,
		Middlewares: []telegram.Middleware{
			waiter,
			ratelimit.New(rate.Every(time.Millisecond*100), 5),
		},
	}
	client := telegram.NewClient(appID, appHash, options)
	api := client.API()
	resolver := peer.Plain(api)
	_ = resolver

	flow := auth.NewFlow(examples.Terminal{PhoneNumber: phone}, auth.SendCodeOptions{})

	return waiter.Run(ctx, func(ctx context.Context) error {
		if err := client.Run(ctx, func(ctx context.Context) error {
			if err := client.Auth().IfNecessary(ctx, flow); err != nil {
				return errors.Wrap(err, "auth")
			}

			// Now that we're authenticated, resolve any usernames
			whitelistStr := cfg.GetString("TELEGRAM_CHANNEL_WHITELIST")
			var whitelistedChannels []int64

			if whitelistStr != "" {
				// Split the string by commas
				items := strings.Split(whitelistStr, ",")
				for _, item := range items {
					// Trim whitespace
					item = strings.TrimSpace(item)
					
					// Try parsing as numeric ID first
					if id, err := strconv.ParseInt(item, 10, 64); err == nil {
						whitelistedChannels = append(whitelistedChannels, id)
						continue
					}
					
					// If not numeric, try resolving as username
					if strings.HasPrefix(item, "@") {
						channelID, err := resolveChannelID(ctx, api, item)
						if err != nil {
							log.Printf("WARNING: Failed to resolve channel %s: %v", item, err)
							continue
						}
						whitelistedChannels = append(whitelistedChannels, channelID)
						log.Printf("Resolved channel %s to ID %d", item, channelID)
					}
				}
			}

			if len(whitelistedChannels) == 0 {
				return errors.New("no valid channel IDs in whitelist")
			}
			// Create message handler with resolved channel IDs
			messageHandler := usecase.NewMessageHandler(
				cfg.GetString("UPLOAD_ENDPOINT"),
				cfg.GetString("SAVE_LOCAL_PATH"),
				cfg.GetString("AUTH_TOKEN"),
				whitelistedChannels,
			)

			// Register handler for new channel messages
			dispatcher.OnNewChannelMessage(messageHandler.HandleChannelMessage)

			self, err := client.Self(ctx)
			if err != nil {
				return errors.Wrap(err, "call self")
			}

			name := self.FirstName
			if self.Username != "" {
				name = fmt.Sprintf("%s (@%s)", name, self.Username)
			}
			fmt.Println("Current user:", name)

			fmt.Println("Listening for updates. Interrupt (Ctrl+C) to stop.")
			return updatesRecovery.Run(ctx, api, self.ID, updates.AuthOptions{
				IsBot: self.Bot,
				OnStart: func(ctx context.Context) {
					fmt.Println("Update recovery initialized and started, listening for events")
				},
			})
		}); err != nil {
			return errors.Wrap(err, "run")
		}
		return nil
	})
}

func sessionFolder(phone string) string {
	var out []rune
	for _, r := range phone {
		if r >= '0' && r <= '9' {
			out = append(out, r)
		}
	}
	return "phone-" + string(out)
}
