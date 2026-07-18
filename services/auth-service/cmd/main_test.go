package main

import (
	"net/http/httptest"
	"testing"
)

func TestParseListParams_Defaults(t *testing.T) {
	r := httptest.NewRequest("GET", "/users", nil)
	p, err := parseListParams(r, userSortColumns, "createdAt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.page != 1 || p.pageSize != 20 || p.sortCol != "created_at" || p.sortDir != "DESC" {
		t.Fatalf("unexpected defaults: %+v", p)
	}
}

func TestParseListParams_RejectsBadInput(t *testing.T) {
	for _, url := range []string{
		"/users?page=0",
		"/users?pageSize=101",
		"/users?sortBy=password_hash", // must not be sortable/selectable
		"/users?sortDir=sideways",
	} {
		r := httptest.NewRequest("GET", url, nil)
		if _, err := parseListParams(r, userSortColumns, "createdAt"); err == nil {
			t.Errorf("expected error for %s", url)
		}
	}
}

func TestParseListParams_ExplicitSort(t *testing.T) {
	r := httptest.NewRequest("GET", "/users?sortBy=email&sortDir=asc", nil)
	p, err := parseListParams(r, userSortColumns, "createdAt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.sortCol != "email" || p.sortDir != "ASC" {
		t.Fatalf("unexpected params: %+v", p)
	}
}
