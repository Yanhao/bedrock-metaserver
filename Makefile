default: metaserver mscli

metaserver: cmd/server/metaserver.go
	rm -f proto/*.cc proto/*.h
	go build $^

mscli: cmd/client/mscli.go
	rm -f proto/*.cc proto/*.h
	go build $^

clean:
	rm -f metaserver mscli

.PHONY: clean proto metaserver mscli
