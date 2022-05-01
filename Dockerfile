# syntax=docker/dockerfile:1.4


###############################################################################
# Flatbuffer Image #
FROM debian:bullseye as flatc

RUN apt-get update && \
    apt-get install -y curl zip unzip 

RUN curl https://github.com/google/flatbuffers/releases/download/v2.0.0/Linux.flatc.binary.clang++-9.zip -L -o /tmp/flatc.zip \
    && unzip /tmp/flatc.zip -d /usr/local/bin \
    && chmod +x /usr/local/bin/flatc \
    && rm /tmp/flatc.zip
COPY ./fbs /fbs
ARG UID
CMD flatc --go --grpc -o /codegen/go /fbs/*.fbs && chown -R ${UID}:${UID} /codegen/


###############################################################################
# Build Image #

FROM golang:1.18-bullseye AS build

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app
ENV CGO_ENABLED=0
COPY go.* /app/
RUN --mount=type=cache,target=/go/pkg/mod \ 
    go mod download

COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
   GOOS=${TARGETOS} GOARCH=${TARGETARCH} make all -f build.makefile -j $(nproc)

###############################################################################
# Runtime Image #

FROM debian:bullseye as runtime

RUN apt-get update && \
    apt-get install ca-certificates -y && \
    apt-get install vim -y && \
    apt-get install curl -y && \
    apt-get install netcat -y && \
    apt-get install zip -y && \
    apt-get install unzip -y && \
    apt-get install less -y && \
    apt-get install groff -y

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install

RUN curl -L -o nerdctl-0.19.0-linux-amd64.tar.gz https://github.com/containerd/nerdctl/releases/download/v0.19.0/nerdctl-0.19.0-linux-amd64.tar.gz && \
    tar -xvf nerdctl-0.19.0-linux-amd64.tar.gz && \
    mv nerdctl /usr/bin/nerdctl

WORKDIR /app
COPY --from=build /app/bin/runtime /app/runtime
ENTRYPOINT /app/runtime

###############################################################################
# Reader Image #

FROM debian:bullseye as reader

WORKDIR /app
COPY --from=build /app/bin/reader /app/bin/reader
ENTRYPOINT /app/bin/reader
