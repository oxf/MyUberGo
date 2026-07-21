package domain

import "testing"

func cands(ids ...string) []Candidate {
	out := make([]Candidate, len(ids))
	for i, id := range ids {
		out[i] = Candidate{DriverID: id, Rating: 5 - float64(i)*0.1}
	}
	return out
}

func ids(cs []Candidate) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.DriverID
	}
	return out
}

func TestSelectOfferTargets(t *testing.T) {
	tests := []struct {
		name       string
		candidates []Candidate
		offered    map[string]bool
		excluded   map[string]bool
		max        int
		want       []string
	}{
		{"caps at max, keeps order", cands("a", "b", "c", "d"), nil, nil, 2, []string{"a", "b"}},
		{"skips already offered", cands("a", "b", "c"), map[string]bool{"a": true}, nil, 5, []string{"b", "c"}},
		{"skips excluded (busy or rate-limited)", cands("a", "b", "c"), nil, map[string]bool{"b": true}, 5, []string{"a", "c"}},
		{"empty pool", nil, nil, nil, 5, nil},
		{"all filtered", cands("a"), map[string]bool{"a": true}, nil, 5, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ids(SelectOfferTargets(tt.candidates, tt.offered, tt.excluded, tt.max))
			if len(got) != len(tt.want) {
				t.Fatalf("got %v want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("got %v want %v", got, tt.want)
				}
			}
		})
	}
}
