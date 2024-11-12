.PHONY: test
test:
	@echo "Running tests"
	@go test -v ./...

setup:
	go install github.com/rubenv/sql-migrate/...@latest
	go install github.com/golang/mock/mockgen@v1.6.0
	go install github.com/vektra/mockery/v2@latest
	go install github.com/swaggo/swag/cmd/swag@v1.8.6
	go install github.com/cosmtrek/air@latest
	cp .env.sample .env
	make init

init:
	go install github.com/rubenv/sql-migrate/...@latest
	make remove-infras
	docker-compose up -d
	@echo "Waiting for database connection..."
	@while ! docker exec mochi-postgres pg_isready > /dev/null; do \
		sleep 1; \
	done
	make migrate-up

dev:
	go run cmd/telegram/main.go

migrate-new:
	sql-migrate new -env=local ${name}

migrate-up:
	sql-migrate up -env=local

migrate-down:
	sql-migrate down -env=local