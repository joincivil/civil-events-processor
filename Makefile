POSTGRES_DATA_DIR=postgresdata
POSTGRES_DOCKER_IMAGE=circleci/postgres:9.6-alpine
POSTGRES_PORT=5432
POSTGRES_DB_NAME=civil_crawler
POSTGRES_USER=docker
POSTGRES_PSWD=docker

PUBSUB_SIM_DOCKER_IMAGE=kinok/google-pubsub-emulator:latest

GOCMD=go
GOGEN=$(GOCMD) generate
GORUN=$(GOCMD) run
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOCOVER=$(GOCMD) tool cover

GO:=$(shell command -v go 2> /dev/null)
DOCKER:=$(shell command -v docker 2> /dev/null)
APT:=$(shell command -v apt-get 2> /dev/null)

## Gometalinter installation
# GOMETALINTER_INSTALLER=scripts/gometalinter_install.sh
# GOMETALINTER_VERSION_TAG=v2.0.11

# curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(go env GOPATH)/bin vX.Y.Z
GOLANGCILINT_URL=https://install.goreleaser.com/github.com/golangci/golangci-lint.sh
GOLANGCILINT_VERSION_TAG=v1.16.0

## Reliant on go and $GOPATH being set
.PHONY: check-go-env
check-go-env:
ifndef GO
	$(error go command is not installed or in PATH)
endif
ifndef GOPATH
	$(error GOPATH is not set)
endif

## NOTE: If installing on a Mac, use Docker for Mac, not Docker toolkit
## https://www.docker.com/docker-mac
.PHONY: check-docker-env
check-docker-env:
ifndef DOCKER
	$(error docker command is not installed or in PATH)
endif

.PHONY: install-dep
install-dep: check-go-env ## Installs dep
	@mkdir -p $(GOPATH)/bin
	@curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh

.PHONY: install-linter
install-linter: check-go-env ## Installs linter
	sh $(GOMETALINTER_INSTALLER) -b $(GOPATH)/bin $(GOMETALINTER_VERSION_TAG)
		@curl -sfL $(GOLANGCILINT_URL) | sh -s -- -b $(shell go env GOPATH)/bin $(GOLANGCILINT_VERSION_TAG)
ifdef APT
	@sudo apt-get install golang-race-detector-runtime || true
endif

.PHONY: install-cover
install-cover: check-go-env ## Installs code coverage tool
	@$(GOGET) -u golang.org/x/tools/cmd/cover

.PHONY: install-gqlgen
install-gqlgen: ## Installs gqlgen graphql library (Not installed with setup)
	@$(GOGET) -u github.com/99designs/gqlgen

.PHONY: install-gorunpkg
install-gorunpkg: ## Installs the gorunpkg command
	@$(GOGET) -u github.com/vektah/gorunpkg

.PHONY: setup
setup: check-go-env install-dep install-linter install-cover install-gorunpkg ## Sets up the tooling.

.PHONY: postgres-setup-launch
postgres-setup-launch:
ifeq ("$(wildcard $(POSTGRES_DATA_DIR))", "")
	mkdir -p $(POSTGRES_DATA_DIR)
	docker run \
		-v $$PWD/$(POSTGRES_DATA_DIR):/tmp/$(POSTGRES_DATA_DIR) -i -t $(POSTGRES_DOCKER_IMAGE) \
		/bin/bash -c "cp -rp /var/lib/postgresql /tmp/$(POSTGRES_DATA_DIR)"
endif
	docker run -e "POSTGRES_USER="$(POSTGRES_USER) -e "POSTGRES_PASSWORD"=$(POSTGRES_PSWD) -e "POSTGRES_DB"=$(POSTGRES_DB_NAME) \
	    -v $$PWD/$(POSTGRES_DATA_DIR)/postgresql:/var/lib/postgresql -d -p $(POSTGRES_PORT):$(POSTGRES_PORT) \
		$(POSTGRES_DOCKER_IMAGE);

.PHONY: postgres-check-available
postgres-check-available:
	@for i in `seq 1 10`; \
	do \
		nc -z localhost 5432 2> /dev/null && exit 0; \
		sleep 3; \
	done; \
	exit 1;

.PHONY: postgres-start
postgres-start: check-docker-env postgres-setup-launch postgres-check-available ## Starts up a development PostgreSQL server
	@echo "Postgresql launched and available"

.PHONY: postgres-stop
postgres-stop: check-docker-env ## Stops the development PostgreSQL server
	@docker stop `docker ps -q`
	@echo 'Postgres stopped'

.PHONY: pubsub-setup-launch
pubsub-setup-launch:
	@docker run -it -d -p 8042:8042 $(PUBSUB_SIM_DOCKER_IMAGE)

.PHONY: pubsub-start
pubsub-start: check-docker-env pubsub-setup-launch ## Starts up the pubsub simulator
	@echo 'Google pubsub simulator up'

.PHONY: pubsub-stop
pubsub-stop: check-docker-env ## Stops the pubsub simulator
	@docker stop `docker ps -q --filter "ancestor=$(PUBSUB_SIM_DOCKER_IMAGE)"`
	@echo 'Google pubsub simulator down'

## golangci-lint config in .golangci.yml
.PHONY: lint
lint: ## Runs linting.
	@golangci-lint run ./...

.PHONY: build
build: ## Builds the main file
	$(GOBUILD) -o ./build/processor cmd/processor/main.go

.PHONY: test
test: ## Runs unit tests and tests code coverage
	@echo 'mode: atomic' > coverage.txt && $(GOTEST) -covermode=atomic -coverprofile=coverage.txt -v -race -timeout=10m ./...

.PHONY: test-integration
test-integration: ## Runs tagged integration tests
	@echo 'mode: atomic' > coverage.txt && PUBSUB_EMULATOR_HOST=localhost:8042 $(GOTEST) -covermode=atomic -coverprofile=coverage.txt -v -race -timeout=10m -tags=integration ./...

.PHONY: cover
cover: test ## Runs unit tests, code coverage, and runs HTML coverage tool.
	@$(GOCOVER) -html=coverage.txt

.PHONY: clean
clean: ## go clean and clean up of artifacts
	@$(GOCLEAN) ./... || true
	@rm coverage.txt || true
	@rm build || true

## Some magic from http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

