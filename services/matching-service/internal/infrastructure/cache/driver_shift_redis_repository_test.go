package cache

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestOnlineRatings_ReturnsScoresForOnlineDrivers_ZeroForOthers(t *testing.T) {
	ctx := context.Background()
	repo := NewDriverRepository(testRedis)

	if err := testRedis.ZAdd(ctx, onlineDriversKey,
		redis.Z{Score: 4.7, Member: "driver-online-ratings-x"},
		redis.Z{Score: 3.2, Member: "driver-online-ratings-y"},
	).Err(); err != nil {
		t.Fatalf("seed drivers:online: %v", err)
	}

	got, err := repo.OnlineRatings(ctx, []string{
		"driver-online-ratings-x", "driver-online-ratings-y", "driver-online-ratings-never-seen",
	})
	if err != nil {
		t.Fatalf("online ratings: %v", err)
	}

	if got["driver-online-ratings-x"] != 4.7 {
		t.Errorf("driver-online-ratings-x rating = %v, want 4.7", got["driver-online-ratings-x"])
	}
	if got["driver-online-ratings-y"] != 3.2 {
		t.Errorf("driver-online-ratings-y rating = %v, want 3.2", got["driver-online-ratings-y"])
	}
	if got["driver-online-ratings-never-seen"] != 0 {
		t.Errorf("driver-online-ratings-never-seen rating = %v, want 0 (not a member)", got["driver-online-ratings-never-seen"])
	}
}

func TestOnlineRatings_EmptyInput(t *testing.T) {
	repo := NewDriverRepository(testRedis)

	got, err := repo.OnlineRatings(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %v, want empty map", got)
	}
}
