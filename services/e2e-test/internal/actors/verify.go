package actors

import (
	"fmt"
	"math"
	"strings"
)

// Verify accumulates field mismatches from one deep-verification pass so a
// single stats event can report everything that was wrong at once.
type Verify struct {
	mismatches []string
}

func (v *Verify) Eq(field string, got, want any) {
	if got != want {
		v.mismatches = append(v.mismatches, fmt.Sprintf("%s: got %v, want %v", field, got, want))
	}
}

func (v *Verify) EqFloat(field string, got, want float64) {
	if math.Abs(got-want) > 1e-9 {
		v.mismatches = append(v.mismatches, fmt.Sprintf("%s: got %v, want %v", field, got, want))
	}
}

func (v *Verify) True(field string, cond bool, detail string) {
	if !cond {
		v.mismatches = append(v.mismatches, fmt.Sprintf("%s: %s", field, detail))
	}
}

func (v *Verify) NotEmpty(field string, got string) {
	if got == "" {
		v.mismatches = append(v.mismatches, field+": empty")
	}
}

func (v *Verify) OK() bool {
	return len(v.mismatches) == 0
}

func (v *Verify) Detail() string {
	return strings.Join(v.mismatches, "; ")
}
