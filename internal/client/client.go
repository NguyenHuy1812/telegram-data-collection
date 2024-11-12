package client

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/NguyenHuy1812/telegram-data-collection/internal/repository"

	"github.com/go-faster/errors"
	"go.etcd.io/bbolt"
	"golang.org/x/time/rate"

	boltstor "github.com/gotd/contrib/bbolt"
	"github.com/gotd/contrib/middleware/floodwait"
	"github.com/gotd/contrib/middleware/ratelimit"

	"github.com/consolelabs/mochi-toolkit/config"
	"github.com/gotd/td/examples"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/message/peer"
	"github.com/gotd/td/telegram/updates"
	"github.com/gotd/td/tg"
	"github.com/NguyenHuy1812/telegram-data-collection/internal/usecase"
)

// Run starts the Telegram client with the provided repository
func Run(ctx context.Context, repo repository.MessageRepository) error {
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

	// Create message handler with repository
	messageHandler := usecase.NewMessageHandler(repo)

	// Register handler for new channel messages
	dispatcher.OnNewChannelMessage(messageHandler.HandleChannelMessage)

	flow := auth.NewFlow(examples.Terminal{PhoneNumber: phone}, auth.SendCodeOptions{})

	return waiter.Run(ctx, func(ctx context.Context) error {
		if err := client.Run(ctx, func(ctx context.Context) error {
			if err := client.Auth().IfNecessary(ctx, flow); err != nil {
				return errors.Wrap(err, "auth")
			}

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

