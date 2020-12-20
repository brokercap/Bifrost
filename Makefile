# Example:
#   make build
#   make clean
prefix=./target

.PHONY: build-all
build-all:
	./build.sh init

build:
	./build.sh linux

install:
	./build.sh init
	./build.sh install $(prefix)

init:
	./build.sh init

clean:
	go clean
	./build.sh clean
