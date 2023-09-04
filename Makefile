default: metaserver mscli

metaserver: main.go
	make -C proto
	cp proto/metaserver.pb.go clients/metaserver/
	cp proto/metaserver_grpc.pb.go clients/metaserver/

	cp proto/dataserver.pb.go clients/dataserver/
	cp proto/dataserver_grpc.pb.go clients/dataserver/

	rm -f proto/*.cc proto/*.h
	go build -race

mscli: cmd/client/mscli.go
	rm -f proto/*.cc proto/*.h
	go build $^

clean:
	rm -f bedrock-metaserver mscli
	make clean -C proto

.PHONY: clean proto metaserver mscli
