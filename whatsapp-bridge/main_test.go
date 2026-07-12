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

func TestMediaFileChecksumStreamsLargeFileAndChecksLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large-media.bin")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	hasher := sha256.New()
	block := make([]byte, 64*1024)
	for index := range block {
		block[index] = byte(index % 251)
	}
	const blockCount = 256 // 16 MiB without a whole-file test allocation.
	for range blockCount {
		if _, err := file.Write(block); err != nil {
			file.Close()
			t.Fatal(err)
		}
		if _, err := hasher.Write(block); err != nil {
			file.Close()
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	expectedLength := uint64(len(block) * blockCount)
	if !mediaFileMatchesSHA256AndLength(path, hasher.Sum(nil), expectedLength) {
		t.Fatal("expected streaming checksum and stat length to match large media")
	}
	if mediaFileMatchesSHA256AndLength(path, hasher.Sum(nil), expectedLength+1) {
		t.Fatal("expected stat length mismatch to reject media before serving")
	}
}

func TestMatchingLegacyMediaMigratesAtomicallyAndPrivately(t *testing.T) {
	chatDir := t.TempDir()
	filename := "legacy-voice-note.ogg"
	legacyPath := legacyMediaLocalPath(chatDir, filename)
	localPath := mediaLocalPath(chatDir, filename, "message-A")
	data := []byte("decrypted legacy voice-note bytes")
	expected := sha256.Sum256(data)
	if err := os.WriteFile(legacyPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	migrated, err := migrateLegacyMediaFile(legacyPath, localPath, expected[:], uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if !migrated {
		t.Fatal("expected matching legacy media to migrate")
	}
	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatalf("legacy file must be removed only after verified migration, stat err=%v", err)
	}
	if !mediaFileMatchesSHA256AndLength(localPath, expected[:], uint64(len(data))) {
		t.Fatal("migrated message-ID cache did not retain the verified bytes")
	}
	info, err := os.Stat(localPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("migrated cache must be private, got mode %04o", info.Mode().Perm())
	}
}

func TestMismatchedLegacyMediaIsRejectedWithoutPoisoningNewCache(t *testing.T) {
	chatDir := t.TempDir()
	filename := "colliding-voice-note.ogg"
	legacyPath := legacyMediaLocalPath(chatDir, filename)
	localPath := mediaLocalPath(chatDir, filename, "message-A")
	legacyData := []byte("bytes belonging to another message")
	expected := sha256.Sum256([]byte("expected message A bytes"))
	if err := os.WriteFile(legacyPath, legacyData, 0644); err != nil {
		t.Fatal(err)
	}

	migrated, err := migrateLegacyMediaFile(legacyPath, localPath, expected[:], uint64(len(legacyData)))
	if err != nil {
		t.Fatal(err)
	}
	if migrated {
		t.Fatal("must not migrate legacy bytes with another message's checksum")
	}
	if _, err := os.Stat(localPath); !os.IsNotExist(err) {
		t.Fatalf("mismatch must not populate the message-ID cache, stat err=%v", err)
	}
	if _, err := os.Stat(legacyPath); err != nil {
		t.Fatalf("mismatched legacy evidence must remain untouched, got %v", err)
	}
}

func TestValidNewMediaCacheIsRepairedToPrivateMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "message-bound.ogg")
	data := []byte("valid decrypted media")
	expected := sha256.Sum256(data)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}

	valid, err := validatedPrivateMediaFile(path, expected[:], uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if !valid {
		t.Fatal("expected valid message-ID cache hit")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("valid cache hit must be chmod 0600, got %04o", info.Mode().Perm())
	}
}

func TestLegacyFilenameCollisionOnlyMigratesMatchingMessage(t *testing.T) {
	chatDir := t.TempDir()
	filename := "same-second.ogg"
	legacyPath := legacyMediaLocalPath(chatDir, filename)
	messageAPath := mediaLocalPath(chatDir, filename, "message-A")
	messageBPath := mediaLocalPath(chatDir, filename, "message-B")
	messageAData := []byte("message A voice note")
	messageBData := []byte("message B voice note")
	messageAHash := sha256.Sum256(messageAData)
	messageBHash := sha256.Sum256(messageBData)
	if err := os.WriteFile(legacyPath, messageBData, 0644); err != nil {
		t.Fatal(err)
	}

	migratedA, err := migrateLegacyMediaFile(
		legacyPath,
		messageAPath,
		messageAHash[:],
		uint64(len(messageAData)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if migratedA {
		t.Fatal("collision must not migrate message B bytes into message A's cache")
	}
	if _, err := os.Stat(messageAPath); !os.IsNotExist(err) {
		t.Fatalf("message A cache was poisoned, stat err=%v", err)
	}

	migratedB, err := migrateLegacyMediaFile(
		legacyPath,
		messageBPath,
		messageBHash[:],
		uint64(len(messageBData)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !migratedB {
		t.Fatal("matching message B should recover the colliding legacy filename")
	}
	if !mediaFileMatchesSHA256AndLength(messageBPath, messageBHash[:], uint64(len(messageBData))) {
		t.Fatal("message B cache did not contain its verified media")
	}
	if messageAPath == messageBPath {
		t.Fatal("message-ID cache paths must remain collision-safe")
	}
}

func TestDownloadMediaUsesVerifiedLegacyCacheBeforeNetwork(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	db, err := sql.Open("sqlite3", filepath.Join(root, "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`
		CREATE TABLE messages (
			id TEXT,
			chat_jid TEXT,
			media_type TEXT,
			filename TEXT,
			url TEXT,
			media_key BLOB,
			file_sha256 BLOB,
			file_enc_sha256 BLOB,
			file_length INTEGER
		)
	`); err != nil {
		t.Fatal(err)
	}
	messageID := "legacy-message"
	chatJID := "chat@lid"
	filename := "legacy.ogg"
	data := []byte("verified legacy voice note")
	expected := sha256.Sum256(data)
	if _, err := db.Exec(
		"INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		messageID,
		chatJID,
		"audio",
		filename,
		"",
		[]byte(nil),
		expected[:],
		[]byte(nil),
		len(data),
	); err != nil {
		t.Fatal(err)
	}
	chatDir := filepath.Join("store", chatJID)
	if err := os.MkdirAll(chatDir, 0755); err != nil {
		t.Fatal(err)
	}
	legacyPath := legacyMediaLocalPath(chatDir, filename)
	if err := os.WriteFile(legacyPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	success, mediaType, returnedFilename, returnedPath, err := downloadMedia(
		nil,
		&MessageStore{db: db},
		messageID,
		chatJID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !success || mediaType != "audio" || returnedFilename != filename {
		t.Fatalf("unexpected migrated download result: success=%v type=%q filename=%q", success, mediaType, returnedFilename)
	}
	expectedPath, err := filepath.Abs(mediaLocalPath(chatDir, filename, messageID))
	if err != nil {
		t.Fatal(err)
	}
	if returnedPath != expectedPath {
		t.Fatalf("download must return message-ID cache path %q, got %q", expectedPath, returnedPath)
	}
	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatalf("legacy file should be removed after /download migration, stat err=%v", err)
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
