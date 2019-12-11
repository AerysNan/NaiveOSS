FROM ubuntu:latest
RUN mkdir -p /workspace/bin
COPY proxy /workspace/bin
WORKDIR /workspace/bin
ENTRYPOINT ["./proxy"]
EXPOSE 8082