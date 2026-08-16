package domain

import "testing"

func TestRankCandidates_NoDistanceFallsBackToRatingOnly(t *testing.T) {
	candidates := []Candidate{
		{DriverID: "a", Rating: 4.0},
		{DriverID: "b", Rating: 4.9},
		{DriverID: "c", Rating: 4.5},
	}
	got := ids(RankCandidates(candidates, false))
	want := []string{"b", "c", "a"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestRankCandidates_ClosestAndHighestRatedWins(t *testing.T) {
	candidates := []Candidate{
		{DriverID: "far-good", Rating: 5.0, DistanceM: 4500},
		{DriverID: "near-good", Rating: 5.0, DistanceM: 100},
		{DriverID: "near-bad", Rating: 1.0, DistanceM: 100},
	}
	got := RankCandidates(candidates, true)
	if got[0].DriverID != "near-good" {
		t.Fatalf("got top candidate %q, want near-good (closest AND highest rated)", got[0].DriverID)
	}
}

// TestRankCandidates_DistanceCarriesWeight guards against ranking collapsing to rating-only when
// hasDistance is true — a far-but-perfect driver must not outrank a close, equally-rated one.
func TestRankCandidates_DistanceCarriesWeight(t *testing.T) {
	candidates := []Candidate{
		{DriverID: "far-perfect", Rating: 5.0, DistanceM: 5000},
		{DriverID: "near-perfect", Rating: 5.0, DistanceM: 0},
	}
	got := RankCandidates(candidates, true)
	if got[0].DriverID != "near-perfect" {
		t.Fatalf("got top candidate %q, want near-perfect", got[0].DriverID)
	}
}

func TestRankCandidates_SingleCandidateReturnedAsIs(t *testing.T) {
	candidates := []Candidate{{DriverID: "solo", Rating: 3.0, DistanceM: 9999}}
	got := RankCandidates(candidates, true)
	if len(got) != 1 || got[0].DriverID != "solo" {
		t.Fatalf("got %+v, want the single input candidate unchanged", got)
	}
}

func TestRankCandidates_Empty(t *testing.T) {
	got := RankCandidates(nil, true)
	if len(got) != 0 {
		t.Fatalf("got %+v, want empty", got)
	}
}

func TestRankCandidates_AllEqualDistanceFallsBackToRating(t *testing.T) {
	candidates := []Candidate{
		{DriverID: "a", Rating: 4.0, DistanceM: 1000},
		{DriverID: "b", Rating: 4.9, DistanceM: 1000},
	}
	got := RankCandidates(candidates, true)
	if got[0].DriverID != "b" {
		t.Fatalf("got top candidate %q, want b (higher rating, distance is a tie)", got[0].DriverID)
	}
}
