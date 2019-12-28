FROM ubuntu:latest
RUN mkdir -p /workspace/bin
RUN mkdir /workspace/data
RUN mkdir /workspace/config
COPY auth /workspace/bin
WORKDIR /workspace/bin
ENTRYPOINT ["./auth"]
EXPOSE 8083