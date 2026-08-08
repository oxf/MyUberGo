package paging

import (
	"net/http/httptest"
	"testing"
)

var testSortColumns = map[string]string{
	"createdAt": "created_at",
	"email":     "email",
}

func TestParseListParams_Defaults(t *testing.T) {
	r := httptest.NewRequest("GET", "/items", nil)
	p, err := ParseListParams(r, testSortColumns, "createdAt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Page != 1 || p.PageSize != 20 || p.SortBy != "createdAt" || p.SortDir != "DESC" {
		t.Fatalf("unexpected defaults: %+v", p)
	}
}

func TestParseListParams_RejectsBadInput(t *testing.T) {
	for _, url := range []string{
		"/items?page=0",
		"/items?pageSize=101",
		"/items?sortBy=password_hash", // not in the whitelist
		"/items?sortDir=sideways",
	} {
		r := httptest.NewRequest("GET", url, nil)
		if _, err := ParseListParams(r, testSortColumns, "createdAt"); err == nil {
			t.Errorf("expected error for %s", url)
		}
	}
}

func TestParseListParams_ExplicitSort(t *testing.T) {
	r := httptest.NewRequest("GET", "/items?sortBy=email&sortDir=asc", nil)
	p, err := ParseListParams(r, testSortColumns, "createdAt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.SortBy != "email" || p.SortDir != "ASC" {
		t.Fatalf("unexpected params: %+v", p)
	}
}
