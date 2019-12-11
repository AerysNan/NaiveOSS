FROM ubuntu:latest
RUN mkdir -p /workspace/bin
RUN mkdir /workspace/data
RUN mkdir /workspace/config
COPY storage /workspace/bin
WORKDIR /workspace/bin
ENTRYPOINT ["./storage"]
EXPOSE 8080