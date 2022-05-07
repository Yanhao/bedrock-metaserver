default: metaserver mscli

metaserver: cmd/server/metaserver.go
	cp proto/metaserver.pb.go service/
	cp proto/metaserver_grpc.pb.go service/

	cp proto/dataserver.pb.go dataserver/
	cp proto/dataserver_grpc.pb.go dataserver/

	rm -f proto/*.cc proto/*.h
	go build $^

mscli: cmd/client/mscli.go
	rm -f proto/*.cc proto/*.h
	go build $^

clean:
	rm -f metaserver mscli

.PHONY: clean proto metaserver mscli
