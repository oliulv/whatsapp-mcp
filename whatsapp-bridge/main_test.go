package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types/events"
)

func TestMediaLocalPathIsBoundToMessageID(t *testing.T) {
	chatDir := filepath.Join("store", "example-chat")
	first := mediaLocalPath(chatDir, "same-second.ogg", "message-A")
	second := mediaLocalPath(chatDir, "same-second.ogg", "message-B")
	if first == second {
		t.Fatalf("different message IDs must not reuse the same media path: %q", first)
	}
	if filepath.Dir(first) != chatDir {
		t.Fatalf("media path escaped chat directory: %q", first)
	}
	if filepath.Ext(first) != ".ogg" {
		t.Fatalf("expected original media extension, got %q", first)
	}
}

func TestMediaFileMatchesStoredPlaintextHash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "voice-note.ogg")
	data := []byte("decrypted voice-note bytes")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	expected := sha256.Sum256(data)
	if !mediaFileMatchesSHA256(path, expected[:]) {
		t.Fatal("expected matching plaintext hash")
	}
	wrong := sha256.Sum256([]byte("different media"))
	if mediaFileMatchesSHA256(path, wrong[:]) {
		t.Fatal("must reject a cached file with the wrong plaintext hash")
	}
}

type fakeConnectionState struct {
	connected bool
	loggedIn  bool
}

func (state fakeConnectionState) IsConnected() bool { return state.connected }
func (state fakeConnectionState) IsLoggedIn() bool  { return state.loggedIn }

func testMessageStore(t *testing.T) *MessageStore {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+t.TempDir()+"/messages.db")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		CREATE TABLE messages (timestamp TIMESTAMP);
		INSERT INTO messages (timestamp) VALUES
			('2026-07-12T09:00:00Z'),
			('2026-07-12T10:30:00Z');
	`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &MessageStore{db: db}
}

func TestPermanentDisconnectBecomesFatal(t *testing.T) {
	err := permanentDisconnectError(&events.StreamReplaced{})
	if err == nil {
		t.Fatal("expected StreamReplaced to terminate the bridge")
	}
	if !strings.Contains(err.Error(), "stream replaced") {
		t.Fatalf("expected disconnect reason in error, got %q", err)
	}

	if err := permanentDisconnectError(&events.Disconnected{}); err != nil {
		t.Fatalf("ordinary disconnects should be left to whatsmeow auto-reconnect, got %v", err)
	}
}

func TestHealthEndpointReportsConnectedSourceWatermark(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	response := httptest.NewRecorder()

	newHealthHandler(
		fakeConnectionState{connected: true, loggedIn: true},
		testMessageStore(t),
	).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected healthy bridge to return 200, got %d: %s", response.Code, response.Body.String())
	}
	var payload struct {
		Status                 string `json:"status"`
		Connected              bool   `json:"connected"`
		LoggedIn               bool   `json:"logged_in"`
		MessageCount           int64  `json:"message_count"`
		LatestMessageTimestamp string `json:"latest_message_timestamp"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Status != "healthy" || !payload.Connected || !payload.LoggedIn {
		t.Fatalf("unexpected connection health: %+v", payload)
	}
	if payload.MessageCount != 2 || payload.LatestMessageTimestamp != "2026-07-12T10:30:00Z" {
		t.Fatalf("unexpected source watermark: %+v", payload)
	}
}

func TestHealthEndpointFailsWhenWhatsAppIsDisconnected(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	response := httptest.NewRecorder()

	newHealthHandler(
		fakeConnectionState{connected: false, loggedIn: false},
		testMessageStore(t),
	).ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected disconnected bridge to return 503, got %d: %s", response.Code, response.Body.String())
	}
	var payload HealthResponse
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Status != "unhealthy" || payload.Connected || payload.LoggedIn {
		t.Fatalf("unexpected disconnected health response: %+v", payload)
	}
}

func TestRESTAPIExposesHealthEndpoint(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	response := httptest.NewRecorder()

	newRESTMux(
		nil,
		fakeConnectionState{connected: true, loggedIn: true},
		testMessageStore(t),
	).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected /api/health route to be healthy, got %d: %s", response.Code, response.Body.String())
	}
}

func TestConnectionWatchdogFailsAfterConsecutiveUnhealthyChecks(t *testing.T) {
	ticks := make(chan time.Time)
	result := make(chan error, 1)
	go func() {
		result <- monitorConnection(
			context.Background(),
			fakeConnectionState{connected: false, loggedIn: false},
			ticks,
			3,
		)
	}()

	for range 3 {
		ticks <- time.Now()
	}

	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "unhealthy") {
			t.Fatalf("expected watchdog failure, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("watchdog did not fail after its unhealthy grace period")
	}
}

func TestRESTServerReportsListenFailures(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port

	server, failures := startRESTServer(nil, testMessageStore(t), port)
	defer server.Close()

	select {
	case err := <-failures:
		if err == nil {
			t.Fatal("expected occupied port to produce a server failure")
		}
	case <-time.After(time.Second):
		t.Fatal("REST server did not report its listen failure")
	}
}

func TestRuntimeExitIsGracefulOnSIGTERM(t *testing.T) {
	signals := make(chan os.Signal, 1)
	signals <- syscall.SIGTERM

	err := waitForRuntimeExit(
		signals,
		make(chan error),
		make(chan error),
		make(chan error),
	)
	if err != nil {
		t.Fatalf("expected SIGTERM to be graceful, got %v", err)
	}
}

func TestRuntimeExitReturnsPermanentDisconnectFailure(t *testing.T) {
	want := permanentDisconnectError(&events.StreamReplaced{})
	failures := make(chan error, 1)
	failures <- want

	err := waitForRuntimeExit(
		make(chan os.Signal),
		failures,
		make(chan error),
		make(chan error),
	)
	if err != want {
		t.Fatalf("expected runtime failure %v, got %v", want, err)
	}
}
