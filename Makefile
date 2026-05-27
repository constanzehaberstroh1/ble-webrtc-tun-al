.PHONY: build-client build-server build-migrate build-web build docker run-server run-client clean tidy migrate sync-web

# Full build: web → binaries
build: build-web build-client build-server build-migrate

# Build frontend and sync to Go embed dir
build-web:
	cd web && npm install && npm run build
	@$(MAKE) sync-web

# Copy web/dist → internal/webui/dist (Go embed source)
sync-web:
	@echo "→ Syncing frontend to internal/webui/dist/"
	rm -rf internal/webui/dist
	cp -r web/dist internal/webui/dist

# Go binaries — always sync frontend first
build-client: sync-web
	go build -o bin/client ./cmd/client

build-server: sync-web
	go build -o bin/server ./cmd/server

build-migrate:
	go build -o bin/migrate ./cmd/migrate

# Docker
docker:
	docker-compose build

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

# Run
run-server:
	sudo ./bin/server

run-client:
	sudo ./bin/client

# Dev: build web + restart client in one step
dev-client: build-web build-client
	@echo "✓ Client rebuilt with fresh frontend"

# Migrate
migrate:
	./bin/migrate --role=server --tokens=.env.tokens

# Maintenance
tidy:
	go mod tidy

clean:
	rm -rf bin/
