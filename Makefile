# Example:
#   make build
#   make clean
prefix=/usr/local/bifrost

.PHONY: build-all
build-all:
	./build.sh init
	./build.sh

build:
	./build.sh

install:
	./build.sh init
	./build.sh install $(prefix)

init:
	./build.sh init

clean:
	go clean
