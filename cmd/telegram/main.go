package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/go-faster/errors"

	"github.com/NguyenHuy1812/telegram-data-collection/internal/client"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Initialize PostgreSQL connection
	if err := client.Run(ctx); err != nil {
		if errors.Is(err, context.Canceled) && ctx.Err() == context.Canceled {
			fmt.Println("\rClosed")
			os.Exit(0)
		}
		_, _ = fmt.Fprintf(os.Stderr, "Error: %+v\n", err)
		os.Exit(1)
	} else {
		fmt.Println("Done")
		os.Exit(0)
	}
}
