# =============================================================================
# Многостадийная сборка Go-агента для маркетинговой платформы
# Использование:
#   docker build --build-arg AGENT=segment -t marketing-agent:segment .
#   docker build --build-arg AGENT=campaign -t marketing-agent:campaign .
#   docker build --build-arg AGENT=analytics -t marketing-agent:analytics .
#   docker build --build-arg AGENT=optimizer -t marketing-agent:optimizer .
# =============================================================================

# ---- Стадия 1: Сборка бинарника ----
FROM golang:1.26-alpine AS builder

ARG AGENT=segment
ARG BUILD_USER=nobody
ARG BUILD_DATE

# Инструменты для сборки.
RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Копируем зависимости конкретного агента для кэширования слоя.
COPY cmd/agents/${AGENT}/go.mod cmd/agents/${AGENT}/go.sum ./
RUN go mod download

# Копируем исходный код агента.
COPY cmd/agents/${AGENT}/ .

# Сборка статического бинарника.
RUN CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    go build \
    -ldflags="-s -w \
    -X main.buildUser=${BUILD_USER} \
    -X main.buildDate=${BUILD_DATE}" \
    -o /app/agent .

# ---- Стадия 2: Минимальный runtime-образ ----
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

# Запуск от непривилегированного пользователя.
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

WORKDIR /app

COPY --from=builder /app/agent /app/agent

EXPOSE 8080

ENTRYPOINT ["/app/agent"]
