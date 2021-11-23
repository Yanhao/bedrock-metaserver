default: mgr mscli

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		messages/message.proto

proto-gogo:
	protoc --gofast_out=plugins=grpc:. messages/message.proto

mgr: cmd/server/mgr.go
	go build $^

mscli: cmd/client/mscli.go
	go build $^

clean:
	rm -f mgr mscli

.PHONY: clean proto
