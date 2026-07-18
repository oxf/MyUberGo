package handler

import (
	"driver-service/internal/domain"
	"net/http/httptest"
	"testing"
)

func TestParseListParams_Defaults(t *testing.T) {
	r := httptest.NewRequest("GET", "/driver-profile", nil)
	p, err := parseListParams(r, domain.DriverProfileSortColumns, "createdAt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.page != 1 || p.pageSize != 20 || p.sortBy != "createdAt" || p.sortDir != "DESC" {
		t.Fatalf("unexpected defaults: %+v", p)
	}
}

func TestParseListParams_RejectsBadInput(t *testing.T) {
	for _, url := range []string{
		"/driver-profile?page=0",
		"/driver-profile?pageSize=0",
		"/driver-profile?pageSize=101",
		"/driver-profile?sortBy=name", // SQL column, not API key — must 400
		"/driver-profile?sortDir=sideways",
	} {
		r := httptest.NewRequest("GET", url, nil)
		if _, err := parseListParams(r, domain.DriverProfileSortColumns, "createdAt"); err == nil {
			t.Errorf("expected error for %s", url)
		}
	}
}

func TestParseListParams_ShiftSortKeys(t *testing.T) {
	r := httptest.NewRequest("GET", "/driver-shift?sortBy=totalEarnings&sortDir=asc", nil)
	p, err := parseListParams(r, domain.ShiftSortColumns, "startedAt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.sortBy != "totalEarnings" || p.sortDir != "ASC" {
		t.Fatalf("unexpected params: %+v", p)
	}
}
