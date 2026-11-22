FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/signalbox ./cmd/signalbox

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/signalbox /signalbox
COPY examples/signalbox.yaml /etc/signalbox/signalbox.yaml
COPY deploy/railway/signalbox.yaml /etc/signalbox/railway.yaml
ENV SIGNALBOX_CONFIG=/etc/signalbox/signalbox.yaml
EXPOSE 8080 4317
USER nonroot
ENTRYPOINT ["/signalbox"]
