FROM ubuntu:latest
RUN mkdir -p /workspace/bin
RUN mkdir /workspace/data
RUN mkdir /workspace/config
COPY metadata /workspace/bin
WORKDIR /workspace/bin
ENTRYPOINT ["./metadata"]
EXPOSE 8081