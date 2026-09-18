PLUGIN_ID := cpa-cache-usage

.PHONY: build test release clean

build:
	CGO_ENABLED=1 go build -trimpath -buildvcs=false -buildmode=c-shared -ldflags="-s -w" -o $(PLUGIN_ID).so .

test:
	go test ./...

release:
	./scripts/package-release.sh

clean:
	rm -f $(PLUGIN_ID).so $(PLUGIN_ID).h
	rm -rf dist
