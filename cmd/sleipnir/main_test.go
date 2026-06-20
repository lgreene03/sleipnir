package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"sleipnir/internal/gateway"
)

// newTestHandler builds the health/admin mux with the given admin token. The
// gateway is constructed with nil collaborators because the admin-auth tests
// exercise only the /admin/* paths, which touch the halt switch and nothing
// else.
func newTestHandler(t *testing.T, adminToken string) (http.Handler, *gateway.Halt) {
	t.Helper()
	tracker := gateway.NewOrderTracker()
	halt := gateway.NewHalt()
	gw := gateway.NewGateway(nil, nil, nil, tracker, nil, nil, halt, 0, slog.Default())
	return healthHandler(tracker, gw, halt, nil, adminToken, slog.Default()), halt
}

func doReq(t *testing.T, h http.Handler, method, path, token, body string) (int, string) {
	t.Helper()
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// When no token is configured, the mutating admin endpoints must fail CLOSED.
func TestAdminFailsClosedWhenTokenUnset(t *testing.T) {
	h, halt := newTestHandler(t, "")

	for _, path := range []string{"/admin/halt", "/admin/resume"} {
		code, _ := doReq(t, h, http.MethodPost, path, "", `{"reason":"x"}`)
		if code != http.StatusServiceUnavailable {
			t.Errorf("POST %s with no configured token = %d; want 503", path, code)
		}
	}
	if halt.IsHalted() {
		t.Error("kill switch should not have engaged when admin is disabled")
	}
}

// With a token configured, an unauthenticated or wrong-token mutation is 401.
func TestAdminRejectsBadToken(t *testing.T) {
	h, halt := newTestHandler(t, "s3cret")

	code, _ := doReq(t, h, http.MethodPost, "/admin/halt", "", `{"reason":"x"}`)
	if code != http.StatusUnauthorized {
		t.Errorf("POST /admin/halt without token = %d; want 401", code)
	}

	code, _ = doReq(t, h, http.MethodPost, "/admin/halt", "wrong", `{"reason":"x"}`)
	if code != http.StatusUnauthorized {
		t.Errorf("POST /admin/halt with wrong token = %d; want 401", code)
	}

	if halt.IsHalted() {
		t.Error("kill switch should not engage on rejected requests")
	}
}

// A valid token engages and clears the kill switch.
func TestAdminValidTokenMutates(t *testing.T) {
	h, halt := newTestHandler(t, "s3cret")

	code, body := doReq(t, h, http.MethodPost, "/admin/halt", "s3cret", `{"reason":"manual"}`)
	if code != http.StatusOK {
		t.Fatalf("POST /admin/halt with valid token = %d; want 200 (body %q)", code, body)
	}
	if !halt.IsHalted() {
		t.Fatal("kill switch should be engaged after authorized halt")
	}

	code, _ = doReq(t, h, http.MethodPost, "/admin/resume", "s3cret", "")
	if code != http.StatusOK {
		t.Fatalf("POST /admin/resume with valid token = %d; want 200", code)
	}
	if halt.IsHalted() {
		t.Fatal("kill switch should be cleared after authorized resume")
	}
}

// Read-only endpoints stay open regardless of token configuration.
func TestReadOnlyEndpointsStayOpen(t *testing.T) {
	h, _ := newTestHandler(t, "s3cret")

	for _, path := range []string{"/healthz", "/admin/halt"} {
		code, _ := doReq(t, h, http.MethodGet, path, "", "")
		if code != http.StatusOK {
			t.Errorf("GET %s = %d; want 200 (should be unauthenticated)", path, code)
		}
	}
}
