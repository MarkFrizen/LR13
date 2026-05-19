package main

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestCacheLifecycle проверяет полный цикл кэширования:
//  1. setCachedSegments → Redis
//  2. loadCacheFromRedis → localCache (имитация перезапуска)
//  3. getCachedSegments → HIT из localCache
//
// Для запуска требуется Redis на localhost:6379.
// Можно пропустить: go test -short ./...
func TestCacheLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("пропуск: требуется Redis")
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	// ────────────────────────────────────────────────────────────
	// 1. Подключение к Redis
	// ────────────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis недоступен (%s): %v", redisAddr, err)
	}
	defer rdb.Close()

	// Очищаем предыдущие тестовые данные.
	rdb.Del(ctx, "segment:test-lifecycle")

	t.Log("Redis подключён")

	// ────────────────────────────────────────────────────────────
	// 2. Сохраняем тестовые сегменты
	// ────────────────────────────────────────────────────────────
	clients := []Client{
		{ID: 1, Name: "Test", Age: 25, Region: "MSK", Purchases: 5},
		{ID: 2, Name: "Test2", Age: 30, Region: "SPB", Purchases: 3},
		{ID: 3, Name: "Test3", Age: 22, Region: "MSK", Purchases: 10},
	}

	testKey := cacheKey(clients)
	expectedSegments := segment(clients)

	t.Logf("ключ кэша: %s", testKey)
	t.Logf("сегментов до кэширования: %d", len(expectedSegments))

	// Сохраняем в Redis.
	setCachedSegments(ctx, rdb, testKey, expectedSegments)

	// Проверяем, что данные появились в Redis.
	exists, err := rdb.Exists(ctx, testKey).Result()
	if err != nil || exists == 0 {
		t.Fatalf("данные не сохранены в Redis: exists=%d err=%v", exists, err)
	}
	ttl, _ := rdb.TTL(ctx, testKey).Result()
	t.Logf("данные в Redis: ttl=%.0fs", ttl.Seconds())

	if ttl <= 0 || ttl > cacheTTL+10*time.Second {
		t.Errorf("некорректный TTL: got=%.0fs want<=%.0fs", ttl.Seconds(), cacheTTL.Seconds())
	}

	// ────────────────────────────────────────────────────────────
	// 3. Проверяем чтение из Redis (имитация второго запроса)
	// ────────────────────────────────────────────────────────────
	gotFromRedis, ok := getCachedSegments(ctx, rdb, testKey)
	if !ok {
		t.Fatal("getCachedSegments вернул false (должен быть HIT из Redis)")
	}
	if len(gotFromRedis) != len(expectedSegments) {
		t.Errorf("getCachedSegments: len=%d, want %d", len(gotFromRedis), len(expectedSegments))
	}
	t.Logf("чтение из Redis: %d сегментов — OK", len(gotFromRedis))

	// Проверяем, что localCache тоже заполнился.
	if _, inLocal := localCache.Load(testKey); !inLocal {
		t.Error("localCache не содержит ключ после getCachedSegments")
	}

	// ────────────────────────────────────────────────────────────
	// 4. Сбрасываем localCache и имитируем перезапуск
	// ────────────────────────────────────────────────────────────
	// Очищаем localCache (имитация потери памяти при останове контейнера).
	localCache = sync.Map{}

	if _, inLocal := localCache.Load(testKey); inLocal {
		t.Error("localCache должен быть пуст после сброса")
	}
	t.Log("localCache сброшен (имитация остановки контейнера)")

	// Загружаем из Redis (имитация loadCacheFromRedis при старте).
	loadCacheFromRedis(rdb)

	// ────────────────────────────────────────────────────────────
	// 5. Проверяем, что данные восстановлены
	// ────────────────────────────────────────────────────────────
	gotFromLocal, ok := localCache.Load(testKey)
	if !ok {
		t.Fatal("localCache не содержит ключ после loadCacheFromRedis — восстановление не сработало")
	}

	segments, ok := gotFromLocal.([]Segment)
	if !ok {
		t.Fatal("значение в localCache не []Segment")
	}
	if len(segments) != len(expectedSegments) {
		t.Errorf("восстановлено сегментов: %d, ожидалось %d", len(segments), len(expectedSegments))
	}

	t.Logf("восстановлено из Redis после перезапуска: %d сегментов — OK", len(segments))

	// ────────────────────────────────────────────────────────────
	// 6. Проверяем, что getCachedSegments теперь берёт из localCache
	// ────────────────────────────────────────────────────────────
	got, ok := getCachedSegments(ctx, rdb, testKey)
	if !ok {
		t.Fatal("getCachedSegments вернул false после восстановления localCache")
	}
	if len(got) != len(expectedSegments) {
		t.Errorf("после восстановления: len=%d, want %d", len(got), len(expectedSegments))
	}
	t.Log("getCachedSegments после восстановления: HIT — OK")

	// Очистка.
	rdb.Del(ctx, testKey)
}

// TestCacheKeyDeterministic проверяет, что cacheKey детерминирован.
func TestCacheKeyDeterministic(t *testing.T) {
	clients := []Client{
		{ID: 1, Name: "Alice", Age: 25, Region: "MSK", Purchases: 5},
	}

	k1 := cacheKey(clients)
	k2 := cacheKey(clients)

	if k1 != k2 {
		t.Errorf("cacheKey не детерминирован: %s != %s", k1, k2)
	}

	if len(k1) != len(cacheKeyPrefix)+32 {
		t.Errorf("неожиданная длина ключа: %d (prefix=%d + 32 hex)", len(k1), len(cacheKeyPrefix))
	}
	t.Logf("cacheKey: %s (len=%d) — OK", k1, len(k1))
}

// TestCacheKeyDifferent проверяет, что разные данные дают разные ключи.
func TestCacheKeyDifferent(t *testing.T) {
	clientsA := []Client{{ID: 1, Name: "A", Age: 20, Region: "MSK", Purchases: 0}}
	clientsB := []Client{{ID: 2, Name: "B", Age: 30, Region: "SPB", Purchases: 5}}

	kA := cacheKey(clientsA)
	kB := cacheKey(clientsB)

	if kA == kB {
		t.Error("cacheKey должен различаться для разных входных данных")
	}
}
