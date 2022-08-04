FROM golang:1.10.0-alpine AS gcsfuse

RUN apk add --no-cache git
ENV GOPATH /go
RUN go get -u github.com/googlecloudplatform/gcsfuse

FROM apache/airflow:2.2.3-python3.7

RUN apk add --no-cache ca-certificates fuse

COPY --from=gcsfuse /go/bin/gcsfuse /usr/local/bin

# Bucket files will be mounted here
RUN mkdir -p /usr/share/airflow/bucket-data

EXPOSE 8080