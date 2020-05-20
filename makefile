n = 200
m = 1000000

all: clean protobuf binary object

protobuf:
	cd proto/metadata && protoc --go_out=plugins=grpc:. *.proto
	cd proto/storage && protoc --go_out=plugins=grpc:. *.proto
	cd proto/auth && protoc --go_out=plugins=grpc:. *.proto
	cd proto/raft && protoc --go_out=plugins=grpc:. *.proto

binary:
	cd cmd/metadata && go build -o ../../build/metadata
	cd cmd/proxy && go build -o ../../build/proxy
	cd cmd/auth && go build -o ../../build/auth
	cd cmd/storage && go build -o ../../build/storage
	cd cmd/client && go build -o ../../build/client

clean:
	rm -f *.obj
	cd proto/metadata && rm -f *.pb.go
	cd proto/storage && rm -f *.pb.go
	cd proto/auth && rm -f *.pb.go
	cd data && rm -rf *

object:
	go run gen.go $(n) $(m)

docker:
	cd cmd/metadata && go build && sudo docker build . -t metadata && rm metadata
	cd cmd/proxy && go build && sudo docker build . -t proxy && rm proxy
	cd cmd/storage && go build && sudo docker build . -t storage && rm storage
	cd cmd/auth && go build && sudo docker build . -t auth && rm auth

test:
	go run cmd/test/parallel.go