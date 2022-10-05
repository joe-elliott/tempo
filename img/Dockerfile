# docker build . -t build-img

ARG ALPINE_VERSION=3.14
ARG GO_VERSION=1.17.1

ARG OTEL_BUILD_PROTOBUF_VERSION=0.11.0
ARG PROTOC_GEN_VT_VERSION=main #v0.2-8-gd852034 # tip of main as of today

FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} as go_builder
RUN apk add --no-cache build-base curl git

ARG PROTOC_GEN_VT_VERSION
RUN mkdir -p ${GOPATH}/src/github.com/planetscale/vtprotobuf && \
    curl -sSL https://api.github.com/repos/planetscale/vtprotobuf/tarball/${PROTOC_GEN_VT_VERSION} | tar xz --strip 1 -C ${GOPATH}/src/github.com/planetscale/vtprotobuf && \
    cd ${GOPATH}/src/github.com/planetscale/vtprotobuf && \
    go build -ldflags '-w -s' -o bin/protoc-gen-go-vtproto ./cmd/protoc-gen-go-vtproto && \
    install -Ds bin/protoc-gen-go-vtproto /out/usr/bin/protoc-gen-go-vtproto

FROM otel/build-protobuf:${OTEL_BUILD_PROTOBUF_VERSION}
COPY --from=go_builder /out/ /