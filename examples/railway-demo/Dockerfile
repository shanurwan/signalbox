FROM golang:1.26-alpine AS build
WORKDIR /src
COPY main.go .
RUN go mod init railway-demo && CGO_ENABLED=0 go build -o /demo .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /demo /demo
ENTRYPOINT ["/demo"]
