package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

type testMediaRetryLIDStore struct {
	pnToLID map[string]types.JID
	lidToPN map[string]types.JID
	err     error
}

func (store *testMediaRetryLIDStore) GetLIDForPN(_ context.Context, pn types.JID) (types.JID, error) {
	if store.err != nil {
		return types.EmptyJID, store.err
	}
	return store.pnToLID[pn.ToNonAD().String()], nil
}

func (store *testMediaRetryLIDStore) GetPNForLID(_ context.Context, lid types.JID) (types.JID, error) {
	if store.err != nil {
		return types.EmptyJID, store.err
	}
	return store.lidToPN[lid.ToNonAD().String()], nil
}

func provenMediaRetryLIDStore(pn, lid types.JID) *testMediaRetryLIDStore {
	return &testMediaRetryLIDStore{
		pnToLID: map[string]types.JID{pn.ToNonAD().String(): lid.ToNonAD()},
		lidToPN: map[string]types.JID{lid.ToNonAD().String(): pn.ToNonAD()},
	}
}

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

func TestExtractDirectPathPreservesMediaQuery(t *testing.T) {
	fullURL := "https://mmg.whatsapp.net/v/t62/example.enc?ccb=11-4&oh=signed-value"
	want := "/v/t62/example.enc?ccb=11-4&oh=signed-value"
	if got := extractDirectPathFromURL(fullURL); got != want {
		t.Fatalf("direct path query was not preserved: got %q want %q", got, want)
	}
	if got := extractDirectPathFromURL(want); got != want {
		t.Fatalf("existing direct path changed: got %q want %q", got, want)
	}
}

func TestExpiredMediaErrorsUseDirectPathFallback(t *testing.T) {
	for _, err := range []error{
		whatsmeow.ErrMediaDownloadFailedWith403,
		whatsmeow.ErrMediaDownloadFailedWith404,
		whatsmeow.ErrMediaDownloadFailedWith410,
	} {
		if !retryableExpiredMediaError(err) {
			t.Fatalf("expected %v to use the direct-path fallback", err)
		}
	}
	if retryableExpiredMediaError(context.Canceled) {
		t.Fatal("context cancellation must not trigger a second media request")
	}
}

func TestMediaRetryRegistersBeforeFastResponse(t *testing.T) {
	coordinator := newMediaRetryCoordinator(1)
	coordinator.decrypt = func(*events.MediaRetry, []byte) (*waMmsRetry.MediaRetryNotification, error) {
		return &waMmsRetry.MediaRetryNotification{
			StanzaID:   proto.String("message-1"),
			DirectPath: proto.String("/fresh/path?token=value"),
			Result:     waMmsRetry.MediaRetryNotification_SUCCESS.Enum(),
		}, nil
	}
	chat := types.NewJID("12345", types.DefaultUserServer)
	info := &types.MessageInfo{
		ID: types.MessageID("message-1"),
		MessageSource: types.MessageSource{
			Chat: chat,
		},
	}
	path, err := coordinator.request(
		context.Background(),
		info,
		[]byte("media-key"),
		func(context.Context, *types.MessageInfo, []byte) error {
			coordinator.handle(&events.MediaRetry{
				MessageID: info.ID,
				ChatID:    info.Chat,
			})
			return nil
		},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if path != "/fresh/path?token=value" {
		t.Fatalf("unexpected refreshed direct path %q", path)
	}
}

func TestDirectMediaRetryAcceptsPNLIDAlternateResponse(t *testing.T) {
	coordinator := newMediaRetryCoordinator(1)
	coordinator.decrypt = func(*events.MediaRetry, []byte) (*waMmsRetry.MediaRetryNotification, error) {
		return &waMmsRetry.MediaRetryNotification{
			StanzaID:   proto.String("message-1"),
			DirectPath: proto.String("/fresh/path?token=value"),
			Result:     waMmsRetry.MediaRetryNotification_SUCCESS.Enum(),
		}, nil
	}
	info := &types.MessageInfo{
		ID: types.MessageID("message-1"),
		MessageSource: types.MessageSource{
			Chat: types.NewJID("12345", types.DefaultUserServer),
		},
	}
	path, err := coordinator.request(
		context.Background(),
		info,
		[]byte("media-key"),
		func(context.Context, *types.MessageInfo, []byte) error {
			coordinator.handle(&events.MediaRetry{
				MessageID: info.ID,
				ChatID:    types.NewJID("67890", types.HiddenUserServer),
			})
			return nil
		},
		nil,
	)
	if err != nil || path != "/fresh/path?token=value" {
		t.Fatalf("PN/LID alternate response was not accepted: path=%q err=%v", path, err)
	}
}

func TestGroupMediaRetryAcceptsProvenPNLIDAlternateSender(t *testing.T) {
	pn := types.NewJID("11111", types.DefaultUserServer)
	lid := types.NewJID("22222", types.HiddenUserServer)
	for _, test := range []struct {
		name     string
		expected types.JID
		actual   types.JID
	}{
		{name: "expected PN response LID", expected: pn, actual: lid},
		{name: "expected LID response PN", expected: lid, actual: pn},
	} {
		t.Run(test.name, func(t *testing.T) {
			coordinator := newMediaRetryCoordinator(1)
			coordinator.decrypt = func(*events.MediaRetry, []byte) (*waMmsRetry.MediaRetryNotification, error) {
				return &waMmsRetry.MediaRetryNotification{
					StanzaID:   proto.String("message-1"),
					DirectPath: proto.String("/fresh/path?token=value"),
					Result:     waMmsRetry.MediaRetryNotification_SUCCESS.Enum(),
				}, nil
			}
			info := &types.MessageInfo{
				ID: types.MessageID("message-1"),
				MessageSource: types.MessageSource{
					Chat:    types.NewJID("12345", types.GroupServer),
					Sender:  test.expected,
					IsGroup: true,
				},
			}
			path, err := coordinator.request(
				context.Background(),
				info,
				[]byte("media-key"),
				func(context.Context, *types.MessageInfo, []byte) error {
					coordinator.handle(&events.MediaRetry{
						MessageID: info.ID,
						ChatID:    info.Chat,
						SenderID:  test.actual,
					})
					return nil
				},
				provenMediaRetryLIDStore(pn, lid),
			)
			if err != nil || path != "/fresh/path?token=value" {
				t.Fatalf("proven PN/LID group sender alternate was not accepted: path=%q err=%v", path, err)
			}
		})
	}
}

func TestGroupMediaRetryRejectsUnprovenOrWrongAlternateSender(t *testing.T) {
	pn := types.NewJID("11111", types.DefaultUserServer)
	lid := types.NewJID("22222", types.HiddenUserServer)
	wrongPN := types.NewJID("33333", types.DefaultUserServer)
	wrongLID := types.NewJID("44444", types.HiddenUserServer)
	tests := []struct {
		name   string
		actual types.JID
		store  mediaRetryLIDStore
	}{
		{name: "missing mapping", actual: lid, store: &testMediaRetryLIDStore{}},
		{name: "wrong sender", actual: wrongLID, store: provenMediaRetryLIDStore(wrongPN, wrongLID)},
		{
			name:   "inconsistent mapping",
			actual: lid,
			store: &testMediaRetryLIDStore{
				pnToLID: map[string]types.JID{pn.String(): lid},
				lidToPN: map[string]types.JID{lid.String(): wrongPN},
			},
		},
		{name: "lookup error", actual: lid, store: &testMediaRetryLIDStore{err: errors.New("lookup failed")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinator := newMediaRetryCoordinator(1)
			info := &types.MessageInfo{
				ID: types.MessageID("message-1"),
				MessageSource: types.MessageSource{
					Chat:    types.NewJID("12345", types.GroupServer),
					Sender:  pn,
					IsGroup: true,
				},
			}
			path, err := coordinator.request(
				context.Background(),
				info,
				[]byte("media-key"),
				func(context.Context, *types.MessageInfo, []byte) error {
					coordinator.handle(&events.MediaRetry{
						MessageID: info.ID,
						ChatID:    info.Chat,
						SenderID:  test.actual,
					})
					return nil
				},
				test.store,
			)
			if err == nil || !strings.Contains(err.Error(), "sender mismatch") || path != "" {
				t.Fatalf("unproven group sender was accepted: path=%q err=%v", path, err)
			}
		})
	}
}

func TestGroupMediaRetryRejectsAlternateChatResponse(t *testing.T) {
	coordinator := newMediaRetryCoordinator(1)
	coordinator.decrypt = func(*events.MediaRetry, []byte) (*waMmsRetry.MediaRetryNotification, error) {
		return &waMmsRetry.MediaRetryNotification{
			StanzaID:   proto.String("message-1"),
			DirectPath: proto.String("/fresh/path?token=value"),
			Result:     waMmsRetry.MediaRetryNotification_SUCCESS.Enum(),
		}, nil
	}
	pn := types.NewJID("11111", types.DefaultUserServer)
	lid := types.NewJID("22222", types.HiddenUserServer)
	info := &types.MessageInfo{
		ID: types.MessageID("message-1"),
		MessageSource: types.MessageSource{
			Chat:    types.NewJID("12345", types.GroupServer),
			Sender:  pn,
			IsGroup: true,
		},
	}
	path, err := coordinator.request(
		context.Background(),
		info,
		[]byte("media-key"),
		func(context.Context, *types.MessageInfo, []byte) error {
			coordinator.handle(&events.MediaRetry{
				MessageID: info.ID,
				ChatID:    types.NewJID("67890", types.GroupServer),
				SenderID:  lid,
			})
			return nil
		},
		provenMediaRetryLIDStore(pn, lid),
	)
	if err == nil || !strings.Contains(err.Error(), "chat mismatch") || path != "" {
		t.Fatalf("alternate group chat response was not rejected by chat identity: path=%q err=%v", path, err)
	}
}

func TestConcurrentMediaRetriesShareOneReceipt(t *testing.T) {
	coordinator := newMediaRetryCoordinator(1)
	coordinator.decrypt = func(*events.MediaRetry, []byte) (*waMmsRetry.MediaRetryNotification, error) {
		return &waMmsRetry.MediaRetryNotification{
			StanzaID:   proto.String("message-1"),
			DirectPath: proto.String("/fresh/path?token=value"),
			Result:     waMmsRetry.MediaRetryNotification_SUCCESS.Enum(),
		}, nil
	}
	info := &types.MessageInfo{
		ID: types.MessageID("message-1"),
		MessageSource: types.MessageSource{
			Chat: types.NewJID("12345", types.DefaultUserServer),
		},
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var sends atomic.Int32
	send := func(context.Context, *types.MessageInfo, []byte) error {
		if sends.Add(1) == 1 {
			close(started)
		}
		<-release
		coordinator.handle(&events.MediaRetry{MessageID: info.ID, ChatID: info.Chat})
		return nil
	}

	var wait sync.WaitGroup
	wait.Add(2)
	errors := make(chan error, 2)
	for range 2 {
		go func() {
			defer wait.Done()
			path, err := coordinator.request(context.Background(), info, []byte("media-key"), send, nil)
			if err == nil && path != "/fresh/path?token=value" {
				err = fmt.Errorf("unexpected refreshed direct path %q", path)
			}
			errors <- err
		}()
	}
	<-started
	close(release)
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	if sends.Load() != 1 {
		t.Fatalf("expected one media retry receipt, got %d", sends.Load())
	}
}

func TestCanceledWaiterDoesNotPoisonSharedMediaRetry(t *testing.T) {
	coordinator := newMediaRetryCoordinator(1)
	coordinator.decrypt = func(*events.MediaRetry, []byte) (*waMmsRetry.MediaRetryNotification, error) {
		return &waMmsRetry.MediaRetryNotification{
			StanzaID:   proto.String("message-1"),
			DirectPath: proto.String("/fresh/path?token=value"),
			Result:     waMmsRetry.MediaRetryNotification_SUCCESS.Enum(),
		}, nil
	}
	info := &types.MessageInfo{
		ID: types.MessageID("message-1"),
		MessageSource: types.MessageSource{
			Chat: types.NewJID("12345", types.DefaultUserServer),
		},
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var sends atomic.Int32
	send := func(context.Context, *types.MessageInfo, []byte) error {
		if sends.Add(1) == 1 {
			close(started)
		}
		<-release
		coordinator.handle(&events.MediaRetry{MessageID: info.ID, ChatID: info.Chat})
		return nil
	}

	firstContext, cancelFirst := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, err := coordinator.request(firstContext, info, []byte("media-key"), send, nil)
		firstResult <- err
	}()
	<-started
	cancelFirst()
	if err := <-firstResult; err == nil || !strings.Contains(err.Error(), "wait canceled") {
		t.Fatalf("expected the first waiter to cancel independently, got %v", err)
	}

	secondResult := make(chan mediaRetryResult, 1)
	go func() {
		path, err := coordinator.request(context.Background(), info, []byte("media-key"), send, nil)
		secondResult <- mediaRetryResult{directPath: path, err: err}
	}()
	close(release)
	result := <-secondResult
	if result.err != nil || result.directPath != "/fresh/path?token=value" {
		t.Fatalf("live waiter did not receive the shared result: path=%q err=%v", result.directPath, result.err)
	}
	if sends.Load() != 1 {
		t.Fatalf("expected one media retry receipt, got %d", sends.Load())
	}
}

func TestMediaRetryRejectsWrongResponseIdentity(t *testing.T) {
	coordinator := newMediaRetryCoordinator(1)
	info := &types.MessageInfo{
		ID: types.MessageID("message-1"),
		MessageSource: types.MessageSource{
			Chat: types.NewJID("12345", types.DefaultUserServer),
		},
	}
	_, err := coordinator.request(
		context.Background(),
		info,
		[]byte("media-key"),
		func(context.Context, *types.MessageInfo, []byte) error {
			coordinator.handle(&events.MediaRetry{
				MessageID: info.ID,
				ChatID:    info.Chat,
				FromMe:    true,
			})
			return nil
		},
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("expected response identity mismatch, got %v", err)
	}
}

func TestGroupMediaRetryUsesExactSessionSender(t *testing.T) {
	messageDB, err := sql.Open("sqlite3", "file:"+filepath.Join(t.TempDir(), "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer messageDB.Close()
	if _, err := messageDB.Exec(`
		CREATE TABLE messages (
			id TEXT,
			chat_jid TEXT,
			sender TEXT,
			sender_jid TEXT,
			is_from_me BOOLEAN
		);
		INSERT INTO messages VALUES ('message-1', 'group-1@g.us', '777', '', 0);
	`); err != nil {
		t.Fatal(err)
	}
	sessionDB, err := sql.Open("sqlite3", "file:"+filepath.Join(t.TempDir(), "session.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer sessionDB.Close()
	if _, err := sessionDB.Exec(`
		CREATE TABLE whatsmeow_message_secrets (
			our_jid TEXT,
			chat_jid TEXT,
			sender_jid TEXT,
			message_id TEXT
		);
		INSERT INTO whatsmeow_message_secrets VALUES
			('111@s.whatsapp.net', 'group-1@g.us', '777@lid', 'message-1'),
			('222@s.whatsapp.net', 'group-1@g.us', '999@lid', 'message-1');
	`); err != nil {
		t.Fatal(err)
	}
	messageStore := &MessageStore{db: messageDB, sessionDB: sessionDB}
	ownJID := types.NewJID("111", types.DefaultUserServer)
	client := &whatsmeow.Client{Store: &store.Device{ID: &ownJID}}
	info, err := mediaRetryMessageInfo(context.Background(), client, messageStore, "message-1", "group-1@g.us")
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsGroup || info.Sender.String() != "777@lid" {
		t.Fatalf("expected exact session sender, got group=%v sender=%q", info.IsGroup, info.Sender)
	}
}

func TestLegacyOutgoingGroupMediaRetryUsesOwnPN(t *testing.T) {
	ownPN := types.NewJID("111", types.DefaultUserServer)
	ownLID := types.NewJID("999", types.HiddenUserServer)
	client := &whatsmeow.Client{Store: &store.Device{ID: &ownPN, LID: ownLID}}
	sender, err := resolveGroupMediaSender(context.Background(), client, "legacy-own-user", "", true)
	if err != nil {
		t.Fatal(err)
	}
	if sender != ownPN {
		t.Fatalf("legacy outgoing group retry must use own PN, got %q", sender)
	}
}

func TestEnsureSenderJIDColumnMigratesExistingMessageStore(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(t.TempDir(), "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE messages (id TEXT)"); err != nil {
		t.Fatal(err)
	}
	if err := ensureMessageColumn(db, "sender_jid", "TEXT"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("PRAGMA table_info(messages)")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		found = found || (name == "sender_jid" && dataType == "TEXT")
	}
	if !found {
		t.Fatal("sender_jid column was not added")
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
	listener, err := net.Listen("tcp", "127.0.0.1:0")
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
