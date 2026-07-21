package domain

// SelectOfferTargets picks up to max candidates to broadcast an offer to.
// candidates must already be ranked best-first (the drivers:online ZSET gives
// rating-desc order). Ranking is rating-only for now — the README's target
// formula (0.4×distance + 0.4×rating + 0.2×acceptance) needs the Location
// service (geo data) and acceptance_rate, neither of which exists yet.
func SelectOfferTargets(candidates []Candidate, alreadyOffered, excluded map[string]bool, max int) []Candidate {
	var out []Candidate
	for _, c := range candidates {
		if len(out) == max {
			break
		}
		if alreadyOffered[c.DriverID] || excluded[c.DriverID] {
			continue
		}
		out = append(out, c)
	}
	return out
}
