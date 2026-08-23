package redisconn

import "testing"

func TestOpen_DefaultURL(t *testing.T) {
	client, err := Open("redis://redis:6379")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := client.Options().Addr; got != "redis:6379" {
		t.Errorf("expected default addr redis:6379, got %q", got)
	}
}

func TestOpen_EnvOverridesURL(t *testing.T) {
	t.Setenv("REDIS_URL", "redis://otherhost:1234")

	client, err := Open("redis://redis:6379")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := client.Options().Addr; got != "otherhost:1234" {
		t.Errorf("expected REDIS_URL override addr otherhost:1234, got %q", got)
	}
}

func TestOpen_PoolSizeDefault(t *testing.T) {
	client, err := Open("redis://redis:6379")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := client.Options().PoolSize; got != 50 {
		t.Errorf("expected default pool size 50, got %d", got)
	}
}

func TestOpen_PoolSizeOverride(t *testing.T) {
	t.Setenv("REDIS_POOL_SIZE", "100")

	client, err := Open("redis://redis:6379")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := client.Options().PoolSize; got != 100 {
		t.Errorf("expected REDIS_POOL_SIZE override 100, got %d", got)
	}
}

func TestOpen_MinIdleConnsDefault(t *testing.T) {
	client, err := Open("redis://redis:6379")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := client.Options().MinIdleConns; got != 10 {
		t.Errorf("expected default min idle conns 10, got %d", got)
	}
}

func TestOpen_MinIdleConnsOverride(t *testing.T) {
	t.Setenv("REDIS_MIN_IDLE_CONNS", "25")

	client, err := Open("redis://redis:6379")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := client.Options().MinIdleConns; got != 25 {
		t.Errorf("expected REDIS_MIN_IDLE_CONNS override 25, got %d", got)
	}
}

func TestOpen_ConnMaxLifetimeDefault(t *testing.T) {
	client, err := Open("redis://redis:6379")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := client.Options().ConnMaxLifetime; got.Minutes() != 5 {
		t.Errorf("expected default conn max lifetime 5m, got %v", got)
	}
}

func TestOpen_ConnMaxLifetimeOverride(t *testing.T) {
	t.Setenv("REDIS_CONN_MAX_LIFETIME_MIN", "15")

	client, err := Open("redis://redis:6379")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := client.Options().ConnMaxLifetime; got.Minutes() != 15 {
		t.Errorf("expected REDIS_CONN_MAX_LIFETIME_MIN override 15m, got %v", got)
	}
}

func TestOpen_InvalidURL_ReturnsError(t *testing.T) {
	t.Setenv("REDIS_URL", "not a valid url")

	client, err := Open("redis://redis:6379")
	if err == nil {
		t.Fatal("expected an error for an invalid REDIS_URL, got nil")
	}
	if client != nil {
		t.Errorf("expected a nil client on error, got %v", client)
	}
}
