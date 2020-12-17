default: mgr mscli

mgr: cmd/server/mgr.go
	go build -mod=vendor $^

mscli: cmd/client/mscli.go
	go build -mod=vendor $^

clean:
	rm -f mgr mscli

.PHONY: clean
