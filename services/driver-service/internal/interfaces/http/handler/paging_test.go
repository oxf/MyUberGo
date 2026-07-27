package handler

import (
	"driver-service/internal/domain"
	"net/http/httptest"
	"testing"
)

func TestParseListParams_Defaults(t *testing.T) {
	r := httptest.NewRequest("GET", "/driver", nil)
	p, err := parseListParams(r, domain.DriverSortColumns, "createdAt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.page != 1 || p.pageSize != 20 || p.sortBy != "createdAt" || p.sortDir != "DESC" {
		t.Fatalf("unexpected defaults: %+v", p)
	}
}

func TestParseListParams_RejectsBadInput(t *testing.T) {
	for _, url := range []string{
		"/driver?page=0",
		"/driver?pageSize=0",
		"/driver?pageSize=101",
		"/driver?sortBy=name", // SQL column, not API key — must 400
		"/driver?sortDir=sideways",
	} {
		r := httptest.NewRequest("GET", url, nil)
		if _, err := parseListParams(r, domain.DriverSortColumns, "createdAt"); err == nil {
			t.Errorf("expected error for %s", url)
		}
	}
}

func TestParseListParams_ShiftSortKeys(t *testing.T) {
	r := httptest.NewRequest("GET", "/driver-shift?sortBy=totalEarningsMinor&sortDir=asc", nil)
	p, err := parseListParams(r, domain.ShiftSortColumns, "startedAt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.sortBy != "totalEarningsMinor" || p.sortDir != "ASC" {
		t.Fatalf("unexpected params: %+v", p)
	}
}
