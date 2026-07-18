package main

import (
	"net/http/httptest"
	"testing"
)

func TestParseListParams_Defaults(t *testing.T) {
	r := httptest.NewRequest("GET", "/ride", nil)
	p, err := parseListParams(r, rideSortColumns, "createdAt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.page != 1 || p.pageSize != 20 || p.sortCol != "created_at" || p.sortDir != "DESC" {
		t.Fatalf("unexpected defaults: %+v", p)
	}
}

func TestParseListParams_RejectsBadInput(t *testing.T) {
	for _, url := range []string{
		"/ride?page=0",
		"/ride?page=-1",
		"/ride?pageSize=0",
		"/ride?pageSize=101",
		"/ride?sortBy=lol",
		"/ride?sortDir=sideways",
	} {
		r := httptest.NewRequest("GET", url, nil)
		if _, err := parseListParams(r, rideSortColumns, "createdAt"); err == nil {
			t.Errorf("expected error for %s", url)
		}
	}
}

func TestParseListParams_ExplicitSortAndPaging(t *testing.T) {
	r := httptest.NewRequest("GET", "/ride?page=3&pageSize=50&sortBy=estimatedPrice&sortDir=asc", nil)
	p, err := parseListParams(r, rideSortColumns, "createdAt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.page != 3 || p.pageSize != 50 || p.sortCol != "estimated_price" || p.sortDir != "ASC" {
		t.Fatalf("unexpected params: %+v", p)
	}
}
