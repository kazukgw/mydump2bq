NAME := mydump2bq
VERSION  := 0.1.0
SRCS    := $(shell find ./cmd/mydump2bq -type f -name '*.go')
LDFLAGS := -ldflags="-s -w -X \"main.VERSION=$(VERSION)\" -extldflags \"-static\""


.PHONY: help
help: ## show this
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'


build: ## build
	go build -buildmode=exe -a -tags netgo -installsuffix netgo $(LDFLAGS) -o $(NAME) $(SRCS)
