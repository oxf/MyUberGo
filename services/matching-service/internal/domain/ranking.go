package domain

import "sort"

type scoredCandidate struct {
	candidate Candidate
	score     float64
}

// RankCandidates scores and sorts candidates best-first: 0.5×normDistance + 0.5×normRating when
// hasDistance, rating-only otherwise; normalization is min-max over this call's own candidate set.
func RankCandidates(candidates []Candidate, hasDistance bool) []Candidate {
	ranked := make([]Candidate, len(candidates))
	copy(ranked, candidates)

	if !hasDistance || len(ranked) <= 1 {
		sort.SliceStable(ranked, func(i, j int) bool { return ranked[i].Rating > ranked[j].Rating })
		return ranked
	}

	minDist, maxDist := ranked[0].DistanceM, ranked[0].DistanceM
	minRating, maxRating := ranked[0].Rating, ranked[0].Rating
	for _, c := range ranked[1:] {
		if c.DistanceM < minDist {
			minDist = c.DistanceM
		}
		if c.DistanceM > maxDist {
			maxDist = c.DistanceM
		}
		if c.Rating < minRating {
			minRating = c.Rating
		}
		if c.Rating > maxRating {
			maxRating = c.Rating
		}
	}

	scored := make([]scoredCandidate, len(ranked))
	for i, c := range ranked {
		// All-equal-distance (or -rating) candidates get a neutral 0.5 for that
		// component rather than a divide-by-zero or arbitrary tie-break.
		normDistance := 0.5
		if maxDist > minDist {
			normDistance = 1 - float64(c.DistanceM-minDist)/float64(maxDist-minDist)
		}
		normRating := 0.5
		if maxRating > minRating {
			normRating = (c.Rating - minRating) / (maxRating - minRating)
		}
		scored[i] = scoredCandidate{candidate: c, score: 0.5*normDistance + 0.5*normRating}
	}

	sort.SliceStable(scored, func(i, j int) bool { return scored[i].score > scored[j].score })

	for i, s := range scored {
		ranked[i] = s.candidate
	}
	return ranked
}
