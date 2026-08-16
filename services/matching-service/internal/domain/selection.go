package domain

// SelectOfferTargets picks up to max candidates to broadcast an offer to. candidates must already
// be ranked best-first (via RankCandidates) before calling this.
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
