package clock_test

import (
	"log"

	"github.com/gotd/td/telegram"

	"github.com/gotd/contrib/clock"
)

func ExampleNewNTP() {
	c, err := clock.NewNTP() // or clock.NewNTP("my.ntp.host")
	if err != nil {
		log.Fatal(err)
	}

	client, err := telegram.ClientFromEnvironment(telegram.Options{
		Clock: c,
	})
	if err != nil {
		log.Fatal(err)
	}

	_ = client
}
