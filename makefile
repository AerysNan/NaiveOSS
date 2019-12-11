n = 10
m = 1024

all: clean protobuf binary object

protobuf:
	cd proto/metadata && protoc --go_out=plugins=grpc:. *.proto
	cd proto/storage && protoc --go_out=plugins=grpc:. *.proto
	cd proto/auth && protoc --go_out=plugins=grpc:. *.proto

binary:
	cd cmd/metadata && go build -o ../../build/metadata
	cd cmd/proxy && go build -o ../../build/proxy
	cd cmd/storage && go build -o ../../build/storage
	cd cmd/client && go build -o ../../build/client
	cd cmd/auth && go build -o ../../build/auth

clean:
	rm -f *.obj
	cd proto/metadata && rm -f *.pb.go
	cd proto/storage && rm -f *.pb.go
	cd proto/auth && rm -f *.pb.go
	cd data && rm -rf *

object:
	python gen.py $(n) $(m)

docker:
	cd cmd/metadata && go build && sudo docker build . -t metadata && rm metadata
	cd cmd/proxy && go build && sudo docker build . -t proxy && rm proxy
	cd cmd/storage && go build && sudo docker build . -t storage && rm storage
	cd cmd/auth && go build && sudo docker build . -t auth && rm auth