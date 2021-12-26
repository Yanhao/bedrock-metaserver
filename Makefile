default: metaserver mscli

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		messages/message.proto
	protoc --go_out=. --go_opt=paths=source_relative metadata/pbdata/data.proto

proto-gogo:
	protoc --gofast_out=plugins=grpc:. messages/message.proto

metaserver: cmd/server/metaserver.go
	go build $^

mscli: cmd/client/mscli.go
	go build $^

clean:
	rm -f metaserver mscli

.PHONY: clean proto metaserver mscli
