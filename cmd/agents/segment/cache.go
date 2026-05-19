package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	cacheTTL         = 1 * time.Hour
	cacheKeyPrefix   = "segment:"
	defaultRedisAddr = "localhost:6379"
	scanBatchSize    = 100
)

// localCache — потокобезопасное in-memory хранилище сегментов.
// Загружается из Redis при старте агента и дублирует каждую запись в Redis.
// Ключ — строка "segment:<hash>", значение — []Segment.
var localCache sync.Map

// initRedis подключается к Redis.
// Порядок определения адреса:
//  1. REDIS_ADDR (формат "host:port", напр. "redis:6379")
//  2. REDIS_URL (формат "redis://user:pass@host:port/db")
//  3. Значение по умолчанию "localhost:6379"
//
// Возвращает nil, если Redis недоступен (агент продолжает работу без кэша).
func initRedis() *redis.Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		redisURL := os.Getenv("REDIS_URL")
		if redisURL != "" {
			opts, err := redis.ParseURL(redisURL)
			if err != nil {
				slog.Warn("не удалось разобрать REDIS_URL, кэш отключён",
					"url", redisURL, "error", err)
				return nil
			}
			rdb := redis.NewClient(opts)
			return pingRedis(rdb, opts.Addr)
		}
		addr = defaultRedisAddr
	}

	opts := &redis.Options{
		Addr: addr,
		DB:   0,
	}
	rdb := redis.NewClient(opts)
	return pingRedis(rdb, addr)
}

// pingRedis проверяет доступность Redis и возвращает клиент или nil.
func pingRedis(rdb *redis.Client, label string) *redis.Client {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Warn("Redis недоступен, кэш отключён", "addr", label, "error", err)
		return nil
	}

	slog.Info("подключение к Redis установлено", "addr", label)
	return rdb
}

// loadCacheFromRedis сканирует Redis по паттерну "segment:*",
// десериализует значения и заполняет localCache.
// Вызывается один раз при старте агента.
func loadCacheFromRedis(rdb *redis.Client) {
	if rdb == nil {
		slog.Warn("Redis не подключён, локальный кэш пуст")
		return
	}

	ctx := context.Background()
	var cursor uint64
	loaded := 0

	for {
		keys, nextCursor, err := rdb.Scan(ctx, cursor, cacheKeyPrefix+"*", scanBatchSize).Result()
		if err != nil {
			slog.Warn("ошибка SCAN Redis", "error", err)
			return
		}

		for _, key := range keys {
			data, err := rdb.Get(ctx, key).Bytes()
			if err != nil {
				continue // ключ мог истечь между SCAN и GET
			}

			var segments []Segment
			if err := json.Unmarshal(data, &segments); err != nil {
				slog.Warn("ошибка десериализации при загрузке кэша",
					"key", key, "error", err)
				continue
			}

			localCache.Store(key, segments)
			loaded++
		}

		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}

	slog.Info("локальный кэш загружен из Redis",
		"keys_loaded", loaded,
	)
}

// cacheKey вычисляет ключ для кэширования результатов сегментации
// на основе SHA256(client data).
func cacheKey(clients []Client) string {
	data, err := json.Marshal(clients)
	if err != nil {
		return cacheKeyPrefix + "error"
	}
	hash := sha256.Sum256(data)
	return cacheKeyPrefix + fmt.Sprintf("%x", hash[:16])
}

// getCachedSegments возвращает сегменты — сначала из localCache, затем из Redis.
// bool = true при успешном чтении.
func getCachedSegments(ctx context.Context, rdb *redis.Client, key string) ([]Segment, bool) {
	// 1. Local cache (in-memory, быстрый доступ).
	if val, ok := localCache.Load(key); ok {
		segments, ok := val.([]Segment)
		if ok {
			slog.Info("кэш LOCAL: HIT", "key", key, "segments", len(segments))
			return segments, true
		}
	}

	// 2. Redis cache.
	if rdb == nil {
		return nil, false
	}

	data, err := rdb.Get(ctx, key).Bytes()
	if err != nil {
		return nil, false
	}

	var segments []Segment
	if err := json.Unmarshal(data, &segments); err != nil {
		slog.Warn("ошибка десериализации кэша", "key", key, "error", err)
		return nil, false
	}

	// Сохраняем в localCache для будущих запросов.
	localCache.Store(key, segments)

	slog.Info("кэш Redis: HIT", "key", key, "segments", len(segments))
	return segments, true
}

// setCachedSegments сохраняет сегменты в Redis (с TTL) и в localCache.
func setCachedSegments(ctx context.Context, rdb *redis.Client, key string, segments []Segment) {
	data, err := json.Marshal(segments)
	if err != nil {
		slog.Warn("ошибка сериализации для кэша", "error", err)
		return
	}

	// Всегда пишем в local cache.
	localCache.Store(key, segments)

	// Пишем в Redis (если доступен).
	if rdb != nil {
		if err := rdb.Set(ctx, key, data, cacheTTL).Err(); err != nil {
			slog.Warn("ошибка записи в Redis", "key", key, "error", err)
			return
		}
	}

	slog.Info("кэш сохранён", "key", key, "ttl", cacheTTL, "segments", len(segments))
}

// closeRedis закрывает соединение с Redis.
// localCache не очищается — данные остаются доступными до завершения процесса.
func closeRedis(rdb *redis.Client) {
	if rdb == nil {
		return
	}
	if err := rdb.Close(); err != nil {
		slog.Error("ошибка закрытия Redis", "error", err)
		return
	}
	slog.Info("соединение с Redis закрыто")
}
