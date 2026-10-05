package health

import (
	"context"
	"errors"
	"testing"
)

type fakePinger struct{ err error }

func (f fakePinger) Ping(ctx context.Context) error { return f.err }

func TestMultiPinger_AllHealthySucceeds(t *testing.T) {
	p := MultiPinger(fakePinger{}, fakePinger{})
	if err := p.Ping(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestMultiPinger_FirstFailureWins(t *testing.T) {
	wantErr := errors.New("redis down")
	p := MultiPinger(fakePinger{err: wantErr}, fakePinger{})
	if err := p.Ping(context.Background()); err != wantErr {
		t.Fatalf("got %v, want %v", err, wantErr)
	}
}

func TestMultiPinger_SecondFailureIsReported(t *testing.T) {
	wantErr := errors.New("postgres down")
	p := MultiPinger(fakePinger{}, fakePinger{err: wantErr})
	if err := p.Ping(context.Background()); err != wantErr {
		t.Fatalf("got %v, want %v", err, wantErr)
	}
}
