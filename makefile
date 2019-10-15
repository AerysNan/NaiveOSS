all: proto

protobuf:
	cd proto/metadata && protoc --go_out=plugins=grpc:. *.proto
	cd proto/storage && protoc --go_out=plugins=grpc:. *.proto

binary:
	cd cmd/metadata && go build -o ../../build
	cd cmd/proxy && go build -o ../../build
	cd cmd/storage && go build -o ../../build

clean:
	cd proto/metadata && rm *.pb.go
	cd proto/storage && rm *.pb.go