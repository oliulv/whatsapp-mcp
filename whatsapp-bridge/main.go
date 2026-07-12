package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	urlpkg "net/url"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mdp/qrterminal"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

const (
	connectionCheckInterval   = 30 * time.Second
	connectionUnhealthyLimit  = 4
	mediaRetryResponseTimeout = 90 * time.Second
	mediaRetrySendTimeout     = 15 * time.Second
	mediaRetryResultTTL       = 5 * time.Minute
)

// Message represents a chat message for our client
type Message struct {
	Time      time.Time
	Sender    string
	Content   string
	IsFromMe  bool
	MediaType string
	Filename  string
}

// Database handler for storing message history
type MessageStore struct {
	db        *sql.DB
	sessionDB *sql.DB
}

type connectionState interface {
	IsConnected() bool
	IsLoggedIn() bool
}

type mediaRetryKey struct {
	chatJID   string
	messageID types.MessageID
}

type mediaRetryResult struct {
	directPath string
	err        error
}

type pendingMediaRetry struct {
	event       chan *events.MediaRetry
	done        chan struct{}
	result      mediaRetryResult
	completedAt time.Time
}

type mediaRetryDecryptFunc func(*events.MediaRetry, []byte) (*waMmsRetry.MediaRetryNotification, error)
type mediaRetrySendFunc func(context.Context, *types.MessageInfo, []byte) error

type mediaRetryCoordinator struct {
	mu      sync.Mutex
	pending map[mediaRetryKey]*pendingMediaRetry
	slots   chan struct{}
	decrypt mediaRetryDecryptFunc
}

func newMediaRetryCoordinator(maxConcurrent int) *mediaRetryCoordinator {
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	return &mediaRetryCoordinator{
		pending: make(map[mediaRetryKey]*pendingMediaRetry),
		slots:   make(chan struct{}, maxConcurrent),
		decrypt: whatsmeow.DecryptMediaRetryNotification,
	}
}

var mediaRetryRequests = newMediaRetryCoordinator(2)

func (coordinator *mediaRetryCoordinator) handle(evt *events.MediaRetry) {
	if coordinator == nil || evt == nil {
		return
	}
	key := mediaRetryKey{chatJID: evt.ChatID.String(), messageID: evt.MessageID}
	coordinator.mu.Lock()
	pending := coordinator.pending[key]
	coordinator.mu.Unlock()
	if pending == nil {
		return
	}
	select {
	case <-pending.done:
		return
	default:
	}
	select {
	case pending.event <- evt:
	default:
	}
}

func (coordinator *mediaRetryCoordinator) complete(
	key mediaRetryKey,
	pending *pendingMediaRetry,
	result mediaRetryResult,
) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if current := coordinator.pending[key]; current != pending {
		return
	}
	pending.result = result
	pending.completedAt = time.Now()
	close(pending.done)
	time.AfterFunc(mediaRetryResultTTL, func() {
		coordinator.mu.Lock()
		defer coordinator.mu.Unlock()
		if coordinator.pending[key] == pending {
			delete(coordinator.pending, key)
		}
	})
}

func (coordinator *mediaRetryCoordinator) request(
	ctx context.Context,
	messageInfo *types.MessageInfo,
	mediaKey []byte,
	send mediaRetrySendFunc,
) (string, error) {
	if coordinator == nil || messageInfo == nil || len(mediaKey) == 0 || send == nil {
		return "", fmt.Errorf("media retry is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	key := mediaRetryKey{chatJID: messageInfo.Chat.String(), messageID: messageInfo.ID}
	coordinator.mu.Lock()
	if existing := coordinator.pending[key]; existing != nil {
		select {
		case <-existing.done:
			if time.Since(existing.completedAt) < mediaRetryResultTTL {
				result := existing.result
				coordinator.mu.Unlock()
				return result.directPath, result.err
			}
			delete(coordinator.pending, key)
		default:
			coordinator.mu.Unlock()
			select {
			case <-existing.done:
				return existing.result.directPath, existing.result.err
			case <-ctx.Done():
				return "", fmt.Errorf("media retry wait canceled: %w", ctx.Err())
			}
		}
	}
	pending := &pendingMediaRetry{
		event: make(chan *events.MediaRetry, 1),
		done:  make(chan struct{}),
	}
	coordinator.pending[key] = pending
	coordinator.mu.Unlock()
	messageInfoCopy := *messageInfo
	mediaKeyCopy := append([]byte(nil), mediaKey...)
	go coordinator.run(key, pending, &messageInfoCopy, mediaKeyCopy, send)

	select {
	case <-pending.done:
		return pending.result.directPath, pending.result.err
	case <-ctx.Done():
		return "", fmt.Errorf("media retry wait canceled: %w", ctx.Err())
	}
}

func (coordinator *mediaRetryCoordinator) run(
	key mediaRetryKey,
	pending *pendingMediaRetry,
	messageInfo *types.MessageInfo,
	mediaKey []byte,
	send mediaRetrySendFunc,
) {
	operationContext, cancelOperation := context.WithTimeout(context.Background(), mediaRetryResponseTimeout)
	defer cancelOperation()
	result := mediaRetryResult{}
	defer func() { coordinator.complete(key, pending, result) }()
	select {
	case coordinator.slots <- struct{}{}:
		defer func() { <-coordinator.slots }()
	case <-operationContext.Done():
		result.err = fmt.Errorf("media retry capacity unavailable")
		return
	}

	sendContext, cancelSend := context.WithTimeout(operationContext, mediaRetrySendTimeout)
	err := send(sendContext, messageInfo, mediaKey)
	cancelSend()
	if err != nil {
		result.err = fmt.Errorf("media retry receipt failed")
		return
	}

	var evt *events.MediaRetry
	select {
	case evt = <-pending.event:
	case <-operationContext.Done():
		result.err = fmt.Errorf("media retry response unavailable")
		return
	}
	if evt == nil || evt.MessageID != messageInfo.ID || evt.ChatID.String() != messageInfo.Chat.String() ||
		evt.FromMe != messageInfo.IsFromMe {
		result.err = fmt.Errorf("media retry response identity mismatch")
		return
	}
	if messageInfo.IsGroup && !messageInfo.Sender.IsEmpty() &&
		evt.SenderID.ToNonAD() != messageInfo.Sender.ToNonAD() {
		result.err = fmt.Errorf("media retry response sender mismatch")
		return
	}
	notification, err := coordinator.decrypt(evt, mediaKey)
	if err != nil {
		result.err = fmt.Errorf("media retry response could not be decrypted")
		return
	}
	if notification == nil || notification.GetResult() != waMmsRetry.MediaRetryNotification_SUCCESS ||
		notification.GetStanzaID() != string(messageInfo.ID) ||
		!strings.HasPrefix(notification.GetDirectPath(), "/") {
		result.err = fmt.Errorf("media retry response did not contain a valid attachment")
		return
	}
	result.directPath = notification.GetDirectPath()
}

type HealthResponse struct {
	Status                 string `json:"status"`
	Connected              bool   `json:"connected"`
	LoggedIn               bool   `json:"logged_in"`
	MessageCount           int64  `json:"message_count"`
	LatestMessageTimestamp string `json:"latest_message_timestamp,omitempty"`
}

func permanentDisconnectError(evt interface{}) error {
	disconnect, ok := evt.(events.PermanentDisconnect)
	if !ok {
		return nil
	}
	return fmt.Errorf("permanent WhatsApp disconnect: %s", disconnect.PermanentDisconnectDescription())
}

func monitorConnection(ctx context.Context, client connectionState, ticks <-chan time.Time, unhealthyLimit int) error {
	consecutiveUnhealthy := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticks:
			connected := client.IsConnected()
			loggedIn := client.IsLoggedIn()
			if connected && loggedIn {
				consecutiveUnhealthy = 0
				continue
			}

			consecutiveUnhealthy++
			if consecutiveUnhealthy >= unhealthyLimit {
				return fmt.Errorf(
					"WhatsApp connection unhealthy for %d consecutive checks (connected=%t, logged_in=%t)",
					consecutiveUnhealthy,
					connected,
					loggedIn,
				)
			}
		}
	}
}

func waitForRuntimeExit(
	signals <-chan os.Signal,
	permanentDisconnects <-chan error,
	watchdogFailures <-chan error,
	serverFailures <-chan error,
) error {
	select {
	case <-signals:
		return nil
	case err := <-permanentDisconnects:
		return err
	case err := <-watchdogFailures:
		return err
	case err := <-serverFailures:
		return err
	}
}

func (store *MessageStore) sourceWatermark() (int64, string, error) {
	var count int64
	var latest string
	err := store.db.QueryRow(
		"SELECT COUNT(*), COALESCE(MAX(timestamp), '') FROM messages",
	).Scan(&count, &latest)
	return count, latest, err
}

func newHealthHandler(client connectionState, messageStore *MessageStore) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count, latest, err := messageStore.sourceWatermark()
		if err != nil {
			http.Error(w, "failed to read message source watermark", http.StatusInternalServerError)
			return
		}

		connected := client.IsConnected()
		loggedIn := client.IsLoggedIn()
		status := "healthy"
		statusCode := http.StatusOK
		if !connected || !loggedIn {
			status = "unhealthy"
			statusCode = http.StatusServiceUnavailable
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_ = json.NewEncoder(w).Encode(HealthResponse{
			Status:                 status,
			Connected:              connected,
			LoggedIn:               loggedIn,
			MessageCount:           count,
			LatestMessageTimestamp: latest,
		})
	})
}

// Initialize message store
func NewMessageStore() (*MessageStore, error) {
	// Create directory for database if it doesn't exist
	if err := os.MkdirAll("store", 0755); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %v", err)
	}

	// Open SQLite database for messages
	db, err := sql.Open("sqlite3", "file:store/messages.db?_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("failed to open message database: %v", err)
	}

	// Create tables if they don't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS chats (
			jid TEXT PRIMARY KEY,
			name TEXT,
			last_message_time TIMESTAMP
		);
		
		CREATE TABLE IF NOT EXISTS messages (
			id TEXT,
			chat_jid TEXT,
			sender TEXT,
			sender_jid TEXT,
			content TEXT,
			timestamp TIMESTAMP,
			is_from_me BOOLEAN,
			media_type TEXT,
			filename TEXT,
			url TEXT,
			media_key BLOB,
			file_sha256 BLOB,
			file_enc_sha256 BLOB,
			file_length INTEGER,
			PRIMARY KEY (id, chat_jid),
			FOREIGN KEY (chat_jid) REFERENCES chats(jid)
		);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}
	if err := ensureMessageColumn(db, "sender_jid", "TEXT"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to migrate message sender identity: %v", err)
	}

	var sessionDB *sql.DB
	if _, statErr := os.Stat("store/whatsapp.db"); statErr == nil {
		sessionDB, err = sql.Open("sqlite3", "file:store/whatsapp.db?mode=ro")
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to open sender identity store: %v", err)
		}
	}

	return &MessageStore{db: db, sessionDB: sessionDB}, nil
}

func ensureMessageColumn(db *sql.DB, column, columnType string) error {
	rows, err := db.Query("PRAGMA table_info(messages)")
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			rows.Close()
			return err
		}
		if name == column {
			found = true
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if found {
		return nil
	}
	if column != "sender_jid" || columnType != "TEXT" {
		return fmt.Errorf("unsupported message column migration")
	}
	_, err = db.Exec("ALTER TABLE messages ADD COLUMN sender_jid TEXT")
	return err
}

// Close the database connection
func (store *MessageStore) Close() error {
	var sessionErr error
	if store.sessionDB != nil {
		sessionErr = store.sessionDB.Close()
	}
	dbErr := store.db.Close()
	if dbErr != nil {
		return dbErr
	}
	return sessionErr
}

// Store a chat in the database
func (store *MessageStore) StoreChat(jid, name string, lastMessageTime time.Time) error {
	_, err := store.db.Exec(
		"INSERT OR REPLACE INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)",
		jid, name, lastMessageTime,
	)
	return err
}

// Store a message in the database
func (store *MessageStore) StoreMessage(id, chatJID, sender, senderJID, content string, timestamp time.Time, isFromMe bool,
	mediaType, filename, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	// Only store if there's actual content or media
	if content == "" && mediaType == "" {
		return nil
	}

	_, err := store.db.Exec(
		`INSERT OR REPLACE INTO messages
		(id, chat_jid, sender, sender_jid, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, chatJID, sender, senderJID, content, timestamp, isFromMe, mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength,
	)
	return err
}

// Get messages from a chat
func (store *MessageStore) GetMessages(chatJID string, limit int) ([]Message, error) {
	rows, err := store.db.Query(
		"SELECT sender, content, timestamp, is_from_me, media_type, filename FROM messages WHERE chat_jid = ? ORDER BY timestamp DESC LIMIT ?",
		chatJID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		var timestamp time.Time
		err := rows.Scan(&msg.Sender, &msg.Content, &timestamp, &msg.IsFromMe, &msg.MediaType, &msg.Filename)
		if err != nil {
			return nil, err
		}
		msg.Time = timestamp
		messages = append(messages, msg)
	}

	return messages, nil
}

// Get all chats
func (store *MessageStore) GetChats() (map[string]time.Time, error) {
	rows, err := store.db.Query("SELECT jid, last_message_time FROM chats ORDER BY last_message_time DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chats := make(map[string]time.Time)
	for rows.Next() {
		var jid string
		var lastMessageTime time.Time
		err := rows.Scan(&jid, &lastMessageTime)
		if err != nil {
			return nil, err
		}
		chats[jid] = lastMessageTime
	}

	return chats, nil
}

// Extract text content from a message
func extractTextContent(msg *waProto.Message) string {
	if msg == nil {
		return ""
	}

	// Try to get text content
	if text := msg.GetConversation(); text != "" {
		return text
	} else if extendedText := msg.GetExtendedTextMessage(); extendedText != nil {
		return extendedText.GetText()
	}

	// For now, we're ignoring non-text messages
	return ""
}

// Extract media info from a message
func extractMediaInfo(msg *waProto.Message) (mediaType string, filename string, url string, mediaKey []byte, fileSHA256 []byte, fileEncSHA256 []byte, fileLength uint64) {
	if msg == nil {
		return "", "", "", nil, nil, nil, 0
	}

	// Check for image message
	if img := msg.GetImageMessage(); img != nil {
		return "image", "image_" + time.Now().Format("20060102_150405") + ".jpg",
			img.GetURL(), img.GetMediaKey(), img.GetFileSHA256(), img.GetFileEncSHA256(), img.GetFileLength()
	}

	// Check for video message
	if vid := msg.GetVideoMessage(); vid != nil {
		return "video", "video_" + time.Now().Format("20060102_150405") + ".mp4",
			vid.GetURL(), vid.GetMediaKey(), vid.GetFileSHA256(), vid.GetFileEncSHA256(), vid.GetFileLength()
	}

	// Check for audio message
	if aud := msg.GetAudioMessage(); aud != nil {
		return "audio", "audio_" + time.Now().Format("20060102_150405") + ".ogg",
			aud.GetURL(), aud.GetMediaKey(), aud.GetFileSHA256(), aud.GetFileEncSHA256(), aud.GetFileLength()
	}

	// Check for document message
	if doc := msg.GetDocumentMessage(); doc != nil {
		filename := doc.GetFileName()
		if filename == "" {
			filename = "document_" + time.Now().Format("20060102_150405")
		}
		return "document", filename,
			doc.GetURL(), doc.GetMediaKey(), doc.GetFileSHA256(), doc.GetFileEncSHA256(), doc.GetFileLength()
	}

	return "", "", "", nil, nil, nil, 0
}

// Handle regular incoming messages with media support
func handleMessage(client *whatsmeow.Client, messageStore *MessageStore, msg *events.Message, logger waLog.Logger) {
	// Save message to database
	chatJID := msg.Info.Chat.String()
	sender := msg.Info.Sender.User

	// Get appropriate chat name (pass nil for conversation since we don't have one for regular messages)
	name := GetChatName(client, messageStore, msg.Info.Chat, chatJID, nil, sender, logger)

	// Update chat in database with the message timestamp (keeps last message time updated)
	err := messageStore.StoreChat(chatJID, name, msg.Info.Timestamp)
	if err != nil {
		logger.Warnf("Failed to store chat: %v", err)
	}

	// Extract text content
	content := extractTextContent(msg.Message)

	// Extract media info
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(msg.Message)

	// Skip if there's no content and no media
	if content == "" && mediaType == "" {
		return
	}

	// Store message in database
	err = messageStore.StoreMessage(
		msg.Info.ID,
		chatJID,
		sender,
		msg.Info.Sender.String(),
		content,
		msg.Info.Timestamp,
		msg.Info.IsFromMe,
		mediaType,
		filename,
		url,
		mediaKey,
		fileSHA256,
		fileEncSHA256,
		fileLength,
	)

	if err != nil {
		logger.Warnf("Failed to store message: %v", err)
	} else {
		// Log message reception
		timestamp := msg.Info.Timestamp.Format("2006-01-02 15:04:05")
		direction := "←"
		if msg.Info.IsFromMe {
			direction = "→"
		}

		// Log based on message type
		if mediaType != "" {
			fmt.Printf("[%s] %s %s: [%s: %s] %s\n", timestamp, direction, sender, mediaType, filename, content)
		} else if content != "" {
			fmt.Printf("[%s] %s %s: %s\n", timestamp, direction, sender, content)
		}
	}
}

// DownloadMediaRequest represents the request body for the download media API
type DownloadMediaRequest struct {
	MessageID string `json:"message_id"`
	ChatJID   string `json:"chat_jid"`
}

// DownloadMediaResponse represents the response for the download media API
type DownloadMediaResponse struct {
	Success  bool   `json:"success"`
	Message  string `json:"message"`
	Filename string `json:"filename,omitempty"`
	Path     string `json:"path,omitempty"`
}

// Store additional media info in the database
func (store *MessageStore) StoreMediaInfo(id, chatJID, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	_, err := store.db.Exec(
		"UPDATE messages SET url = ?, media_key = ?, file_sha256 = ?, file_enc_sha256 = ?, file_length = ? WHERE id = ? AND chat_jid = ?",
		url, mediaKey, fileSHA256, fileEncSHA256, fileLength, id, chatJID,
	)
	return err
}

// Get media info from the database
func (store *MessageStore) GetMediaInfo(id, chatJID string) (string, string, string, []byte, []byte, []byte, uint64, error) {
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64

	err := store.db.QueryRow(
		"SELECT media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length FROM messages WHERE id = ? AND chat_jid = ?",
		id, chatJID,
	).Scan(&mediaType, &filename, &url, &mediaKey, &fileSHA256, &fileEncSHA256, &fileLength)

	return mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err
}

func (store *MessageStore) GetMessageSource(id, chatJID string) (sender, senderJID string, isFromMe bool, err error) {
	err = store.db.QueryRow(
		"SELECT sender, COALESCE(sender_jid, ''), is_from_me FROM messages WHERE id = ? AND chat_jid = ?",
		id, chatJID,
	).Scan(&sender, &senderJID, &isFromMe)
	return
}

func (store *MessageStore) GetExactSessionSender(
	ctx context.Context,
	id, chatJID, ownJID string,
) (string, error) {
	if store.sessionDB == nil || ownJID == "" {
		return "", nil
	}
	rows, err := store.sessionDB.QueryContext(
		ctx,
		`SELECT DISTINCT sender_jid
		 FROM whatsmeow_message_secrets
		 WHERE our_jid = ? AND message_id = ? AND chat_jid = ? AND sender_jid <> ''
		 LIMIT 2`,
		ownJID,
		id,
		chatJID,
	)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var senders []string
	for rows.Next() {
		var sender string
		if err := rows.Scan(&sender); err != nil {
			return "", err
		}
		senders = append(senders, sender)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(senders) > 1 {
		return "", fmt.Errorf("multiple session senders matched one message")
	}
	if len(senders) == 1 {
		return senders[0], nil
	}
	return "", nil
}

func resolveGroupMediaSender(
	ctx context.Context,
	client *whatsmeow.Client,
	sender, senderJID string,
	isFromMe bool,
) (types.JID, error) {
	if senderJID != "" {
		parsed, err := types.ParseJID(senderJID)
		if err != nil || parsed.IsEmpty() {
			return types.EmptyJID, fmt.Errorf("stored group sender identity is invalid")
		}
		return parsed.ToNonAD(), nil
	}
	if strings.Contains(sender, "@") {
		parsed, err := types.ParseJID(sender)
		if err != nil || parsed.IsEmpty() {
			return types.EmptyJID, fmt.Errorf("stored group sender identity is invalid")
		}
		return parsed.ToNonAD(), nil
	}
	if client == nil || client.Store == nil {
		return types.EmptyJID, fmt.Errorf("group sender identity is unavailable")
	}
	if isFromMe {
		if client.Store.ID != nil && !client.Store.ID.IsEmpty() {
			return client.Store.ID.ToNonAD(), nil
		}
		return types.EmptyJID, fmt.Errorf("own group sender identity is unavailable")
	}
	if sender == "" {
		return types.EmptyJID, fmt.Errorf("group sender identity is unavailable")
	}

	lidCandidate := types.NewJID(sender, types.HiddenUserServer)
	pnCandidate := types.NewJID(sender, types.DefaultUserServer)
	mappedPN, lidErr := client.Store.LIDs.GetPNForLID(ctx, lidCandidate)
	mappedLID, pnErr := client.Store.LIDs.GetLIDForPN(ctx, pnCandidate)
	if lidErr != nil || pnErr != nil {
		return types.EmptyJID, fmt.Errorf("group sender identity lookup failed")
	}
	isLID := !mappedPN.IsEmpty()
	isPN := !mappedLID.IsEmpty()
	if isLID == isPN {
		return types.EmptyJID, fmt.Errorf("group sender identity is ambiguous")
	}
	if isLID {
		return lidCandidate, nil
	}
	return pnCandidate, nil
}

func mediaRetryMessageInfo(
	ctx context.Context,
	client *whatsmeow.Client,
	messageStore *MessageStore,
	messageID, chatJID string,
) (*types.MessageInfo, error) {
	chat, err := types.ParseJID(chatJID)
	if err != nil || chat.IsEmpty() {
		return nil, fmt.Errorf("stored media chat identity is invalid")
	}
	sender, senderJID, isFromMe, err := messageStore.GetMessageSource(messageID, chatJID)
	if err != nil {
		return nil, fmt.Errorf("stored media source identity is unavailable")
	}
	info := &types.MessageInfo{
		ID: types.MessageID(messageID),
		MessageSource: types.MessageSource{
			Chat:     chat,
			IsFromMe: isFromMe,
			IsGroup:  chat.Server == types.GroupServer,
		},
	}
	if info.IsGroup {
		if senderJID == "" {
			if client == nil || client.Store == nil || client.Store.ID == nil {
				return nil, fmt.Errorf("active account identity is unavailable")
			}
			senderJID, err = messageStore.GetExactSessionSender(
				ctx,
				messageID,
				chatJID,
				client.Store.ID.String(),
			)
			if err != nil {
				return nil, fmt.Errorf("exact group sender identity lookup failed")
			}
		}
		info.Sender, err = resolveGroupMediaSender(ctx, client, sender, senderJID, isFromMe)
		if err != nil {
			return nil, err
		}
	}
	return info, nil
}

func mediaLocalPath(chatDir, filename, messageID string) string {
	digest := sha256.Sum256([]byte(messageID))
	extension := filepath.Ext(filepath.Base(filename))
	return filepath.Join(chatDir, fmt.Sprintf("%x%s", digest[:12], extension))
}

func mediaFileMatchesSHA256(path string, expected []byte) bool {
	return mediaFileMatchesSHA256AndLength(path, expected, 0)
}

func mediaFileMatchesSHA256AndLength(path string, expected []byte, expectedLength uint64) bool {
	if len(expected) == 0 {
		return false
	}
	pathInfo, err := os.Lstat(path)
	if err != nil || !pathInfo.Mode().IsRegular() {
		return false
	}
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || !os.SameFile(pathInfo, info) {
		return false
	}
	if expectedLength > 0 && (info.Size() < 0 || uint64(info.Size()) != expectedLength) {
		return false
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return false
	}
	return bytes.Equal(hasher.Sum(nil), expected)
}

func writeMediaReaderAtomically(path string, source io.Reader, expected []byte, expectedLength uint64) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".whatsapp-media-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0600); err != nil {
		temporary.Close()
		return err
	}
	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(temporary, hasher), source)
	if err != nil {
		temporary.Close()
		return err
	}
	if expectedLength > 0 && (written < 0 || uint64(written) != expectedLength) {
		temporary.Close()
		return fmt.Errorf("media plaintext length mismatch")
	}
	if len(expected) > 0 && !bytes.Equal(hasher.Sum(nil), expected) {
		temporary.Close()
		return fmt.Errorf("media plaintext checksum mismatch")
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	if err := os.Chmod(path, 0600); err != nil {
		return err
	}
	finalFile, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := finalFile.Sync(); err != nil {
		finalFile.Close()
		return err
	}
	if err := finalFile.Close(); err != nil {
		return err
	}
	return syncMediaDirectory(filepath.Dir(path))
}

func syncMediaDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func writeMediaFileAtomically(path string, data []byte) error {
	return writeMediaReaderAtomically(path, bytes.NewReader(data), nil, uint64(len(data)))
}

func validatedPrivateMediaFile(path string, expected []byte, expectedLength uint64) (bool, error) {
	if !mediaFileMatchesSHA256AndLength(path, expected, expectedLength) {
		return false, nil
	}
	if err := os.Chmod(path, 0600); err != nil {
		return false, err
	}
	return true, nil
}

func legacyMediaLocalPath(chatDir, filename string) string {
	base := filepath.Base(filename)
	if base == "" || base == "." || base == string(filepath.Separator) {
		return ""
	}
	return filepath.Join(chatDir, base)
}

func migrateLegacyMediaFile(legacyPath, localPath string, expected []byte, expectedLength uint64) (bool, error) {
	if legacyPath == "" || filepath.Clean(legacyPath) == filepath.Clean(localPath) {
		return false, nil
	}
	// The legacy filename cache can contain bytes for another message because
	// WhatsApp-generated filenames are not unique. Never copy it unless its
	// decrypted plaintext matches this message's database checksum and length.
	if !mediaFileMatchesSHA256AndLength(legacyPath, expected, expectedLength) {
		return false, nil
	}
	legacy, err := os.Open(legacyPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer legacy.Close()
	if err := writeMediaReaderAtomically(localPath, legacy, expected, expectedLength); err != nil {
		return false, err
	}
	// Verify the final pathname before removing the only legacy copy. This also
	// protects against an unexpected rename/race leaving different bytes behind.
	valid, err := validatedPrivateMediaFile(localPath, expected, expectedLength)
	if err != nil {
		return false, err
	}
	if !valid {
		_ = os.Remove(localPath)
		return false, fmt.Errorf("migrated media failed final checksum verification")
	}
	if err := os.Remove(legacyPath); err != nil && !os.IsNotExist(err) {
		if chmodErr := os.Chmod(legacyPath, 0600); chmodErr != nil {
			return false, fmt.Errorf("could not remove or secure legacy media: %v; chmod: %v", err, chmodErr)
		}
		return true, err
	}
	if err := syncMediaDirectory(filepath.Dir(legacyPath)); err != nil {
		return true, err
	}
	return true, nil
}

// MediaDownloader implements the whatsmeow.DownloadableMessage interface
type MediaDownloader struct {
	URL           string
	DirectPath    string
	MediaKey      []byte
	FileLength    uint64
	FileSHA256    []byte
	FileEncSHA256 []byte
	MediaType     whatsmeow.MediaType
}

// GetDirectPath implements the DownloadableMessage interface
func (d *MediaDownloader) GetDirectPath() string {
	return d.DirectPath
}

// GetURL implements the DownloadableMessage interface
func (d *MediaDownloader) GetURL() string {
	return d.URL
}

// GetMediaKey implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaKey() []byte {
	return d.MediaKey
}

// GetFileLength implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileLength() uint64 {
	return d.FileLength
}

// GetFileSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileSHA256() []byte {
	return d.FileSHA256
}

// GetFileEncSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileEncSHA256() []byte {
	return d.FileEncSHA256
}

// GetMediaType implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaType() whatsmeow.MediaType {
	return d.MediaType
}

// Function to download media from a message
func downloadMedia(client *whatsmeow.Client, messageStore *MessageStore, messageID, chatJID string) (bool, string, string, string, error) {
	return downloadMediaContext(context.Background(), client, messageStore, messageID, chatJID)
}

func downloadMediaContext(
	ctx context.Context,
	client *whatsmeow.Client,
	messageStore *MessageStore,
	messageID, chatJID string,
) (bool, string, string, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Query the database for the message
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64
	var err error

	// First, check if we already have this file
	chatDir := fmt.Sprintf("store/%s", strings.ReplaceAll(chatJID, ":", "_"))
	localPath := ""

	// Get media info from the database
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err = messageStore.GetMediaInfo(messageID, chatJID)

	if err != nil {
		// Try to get basic info if extended info isn't available
		err = messageStore.db.QueryRow(
			"SELECT media_type, filename FROM messages WHERE id = ? AND chat_jid = ?",
			messageID, chatJID,
		).Scan(&mediaType, &filename)

		if err != nil {
			return false, "", "", "", fmt.Errorf("failed to find message: %v", err)
		}
	}

	// Check if this is a media message
	if mediaType == "" {
		return false, "", "", "", fmt.Errorf("not a media message")
	}

	// Create directory for the chat if it doesn't exist
	if err := os.MkdirAll(chatDir, 0755); err != nil {
		return false, "", "", "", fmt.Errorf("failed to create chat directory: %v", err)
	}

	// Bind the local cache path to the WhatsApp message ID. Media filenames are
	// generated with second-level timestamps and are not unique, so using the
	// filename alone can return another message's already-downloaded audio.
	localPath = mediaLocalPath(chatDir, filename, messageID)

	// Get absolute path
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to get absolute path: %v", err)
	}

	// Check the message-ID-bound cache first and repair its permissions before
	// returning a path to another process.
	validLocal, err := validatedPrivateMediaFile(localPath, fileSHA256, fileLength)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to secure cached media file: %v", err)
	}
	if validLocal {
		return true, mediaType, filename, absPath, nil
	}

	// Older bridge builds cached decrypted media at chatDir/filename. Recover a
	// legacy file only when it matches this DB row; filename collisions must
	// never populate or be served from another message's hashed cache path.
	legacyPath := legacyMediaLocalPath(chatDir, filename)
	migrated, migrationErr := migrateLegacyMediaFile(legacyPath, localPath, fileSHA256, fileLength)
	if migrated {
		if migrationErr != nil {
			fmt.Printf("Warning: migrated legacy media for message %s but could not remove the legacy file: %v\n", messageID, migrationErr)
		}
		return true, mediaType, filename, absPath, nil
	}
	if migrationErr != nil {
		return false, "", "", "", fmt.Errorf("failed to migrate legacy media cache: %v", migrationErr)
	}

	// If we don't have all the media info we need, we can't download
	if url == "" || len(mediaKey) == 0 || len(fileSHA256) == 0 || len(fileEncSHA256) == 0 || fileLength == 0 {
		return false, "", "", "", fmt.Errorf("incomplete media information for download")
	}

	fmt.Printf("Attempting to download media for message %s in chat %s...\n", messageID, chatJID)

	// Extract direct path from URL
	directPath := extractDirectPathFromURL(url)

	// Create a downloader that implements DownloadableMessage
	var waMediaType whatsmeow.MediaType
	switch mediaType {
	case "image":
		waMediaType = whatsmeow.MediaImage
	case "video":
		waMediaType = whatsmeow.MediaVideo
	case "audio":
		waMediaType = whatsmeow.MediaAudio
	case "document":
		waMediaType = whatsmeow.MediaDocument
	default:
		return false, "", "", "", fmt.Errorf("unsupported media type: %s", mediaType)
	}

	downloader := &MediaDownloader{
		URL:           url,
		DirectPath:    directPath,
		MediaKey:      mediaKey,
		FileLength:    fileLength,
		FileSHA256:    fileSHA256,
		FileEncSHA256: fileEncSHA256,
		MediaType:     waMediaType,
	}

	// Download the media using whatsmeow client. Stored full URLs carry a
	// short-lived host signature. If that URL has expired, retry the exact
	// message-bound DirectPath through whatsmeow's fresh media connection before
	// considering a protocol-level media retry receipt.
	downloadContext, cancelDownload := context.WithTimeout(ctx, 2*time.Minute)
	mediaData, err := client.Download(downloadContext, downloader)
	if err != nil && retryableExpiredMediaError(err) && directPath != "" {
		directPathDownloader := *downloader
		directPathDownloader.URL = ""
		directPathDownloader.DirectPath = directPath
		mediaData, err = client.Download(downloadContext, &directPathDownloader)
	}
	cancelDownload()
	if err != nil && retryableExpiredMediaError(err) {
		// This encrypted server-error receipt is addressed only to the account's
		// own linked phone. It requests re-upload of the already-received media;
		// it is not a chat message, is never exposed through MCP, and cannot target
		// a contact. One receipt is allowed only after both receive-side paths have
		// returned a terminal expiry response.
		retryContext, cancelRetry := context.WithTimeout(ctx, mediaRetryResponseTimeout)
		messageInfo, infoErr := mediaRetryMessageInfo(
			retryContext,
			client,
			messageStore,
			messageID,
			chatJID,
		)
		if infoErr == nil {
			var refreshedDirectPath string
			refreshedDirectPath, infoErr = mediaRetryRequests.request(
				retryContext,
				messageInfo,
				mediaKey,
				client.SendMediaRetryReceipt,
			)
			if infoErr == nil {
				refreshedDownloader := *downloader
				refreshedDownloader.URL = ""
				refreshedDownloader.DirectPath = refreshedDirectPath
				refreshedDownloadContext, cancelRefreshedDownload := context.WithTimeout(
					ctx,
					2*time.Minute,
				)
				mediaData, err = client.Download(refreshedDownloadContext, &refreshedDownloader)
				cancelRefreshedDownload()
			}
		}
		cancelRetry()
		if infoErr != nil {
			err = fmt.Errorf("expired media recovery unavailable")
		}
	}
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to download media: %v", err)
	}
	actualSHA256 := sha256.Sum256(mediaData)
	if !bytes.Equal(actualSHA256[:], fileSHA256) {
		return false, "", "", "", fmt.Errorf("downloaded media plaintext checksum mismatch")
	}

	// Save the downloaded media atomically so a bridge restart cannot leave a
	// partial file that later appears to be a valid cache hit.
	if err := writeMediaFileAtomically(localPath, mediaData); err != nil {
		return false, "", "", "", fmt.Errorf("failed to save media file: %v", err)
	}

	fmt.Printf("Successfully downloaded %s media to %s (%d bytes)\n", mediaType, absPath, len(mediaData))
	return true, mediaType, filename, absPath, nil
}

// Extract direct path from a WhatsApp media URL
func extractDirectPathFromURL(url string) string {
	// DirectPath includes its query string. whatsmeow appends the current media
	// host plus checksum parameters to this value, so stripping the query turns
	// an otherwise recoverable attachment into another 403.
	parsed, err := urlpkg.Parse(url)
	if err != nil || parsed.Path == "" {
		return url
	}
	directPath := parsed.EscapedPath()
	if parsed.RawQuery != "" {
		directPath += "?" + parsed.RawQuery
	}
	return directPath
}

func retryableExpiredMediaError(err error) bool {
	return errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410)
}

func newRESTMux(client *whatsmeow.Client, health connectionState, messageStore *MessageStore) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/api/health", newHealthHandler(health, messageStore))

	// Handler for downloading media
	mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		// Only allow POST requests
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the request body
		var req DownloadMediaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.MessageID == "" || req.ChatJID == "" {
			http.Error(w, "Message ID and Chat JID are required", http.StatusBadRequest)
			return
		}

		// Download the media
		success, mediaType, filename, path, err := downloadMediaContext(
			r.Context(),
			client,
			messageStore,
			req.MessageID,
			req.ChatJID,
		)

		// Set response headers
		w.Header().Set("Content-Type", "application/json")

		// Handle download result
		if !success || err != nil {
			errMsg := "Unknown error"
			if err != nil {
				errMsg = err.Error()
			}

			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(DownloadMediaResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to download media: %s", errMsg),
			})
			return
		}

		// Send successful response
		json.NewEncoder(w).Encode(DownloadMediaResponse{
			Success:  true,
			Message:  fmt.Sprintf("Successfully downloaded %s media", mediaType),
			Filename: filename,
			Path:     path,
		})
	})
	return mux
}

// Start a REST API server to expose the WhatsApp client functionality
func startRESTServer(client *whatsmeow.Client, messageStore *MessageStore, port int) (*http.Server, <-chan error) {
	mux := newRESTMux(client, client, messageStore)

	// Start the server
	// The bridge REST API can trigger an own-device media retry receipt, so it
	// is a local control plane only. Tailnet clients use the read-only MCP
	// service on its separate port; never expose this endpoint directly.
	serverAddr := fmt.Sprintf("127.0.0.1:%d", port)
	fmt.Printf("Starting REST API server on %s...\n", serverAddr)
	server := &http.Server{Addr: serverAddr, Handler: mux}
	failures := make(chan error, 1)

	// Run server in a goroutine so it doesn't block
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("REST API server error: %v\n", err)
			failures <- err
		}
	}()
	return server, failures
}

func main() {
	// Set up logger
	logger := waLog.Stdout("Client", "INFO", true)
	logger.Infof("Starting WhatsApp client...")

	// Create database connection for storing session data
	dbLog := waLog.Stdout("Database", "INFO", true)

	// Create directory for database if it doesn't exist
	if err := os.MkdirAll("store", 0755); err != nil {
		logger.Errorf("Failed to create store directory: %v", err)
		return
	}

	container, err := sqlstore.New(context.Background(), "sqlite3", "file:store/whatsapp.db?_foreign_keys=on", dbLog)
	if err != nil {
		logger.Errorf("Failed to connect to database: %v", err)
		return
	}

	// Get device store - This contains session information
	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		if err == sql.ErrNoRows {
			// No device exists, create one
			deviceStore = container.NewDevice()
			logger.Infof("Created new device")
		} else {
			logger.Errorf("Failed to get device: %v", err)
			return
		}
	}

	// Create client instance
	client := whatsmeow.NewClient(deviceStore, logger)
	if client == nil {
		logger.Errorf("Failed to create WhatsApp client")
		return
	}

	// Initialize message store
	messageStore, err := NewMessageStore()
	if err != nil {
		logger.Errorf("Failed to initialize message store: %v", err)
		return
	}
	defer messageStore.Close()
	permanentDisconnects := make(chan error, 1)

	// Setup event handling for messages and history sync
	client.AddEventHandler(func(evt interface{}) {
		if err := permanentDisconnectError(evt); err != nil {
			logger.Errorf("%v", err)
			select {
			case permanentDisconnects <- err:
			default:
			}
		}

		switch v := evt.(type) {
		case *events.Message:
			// Process regular messages
			handleMessage(client, messageStore, v, logger)

		case *events.HistorySync:
			// Process history sync events
			handleHistorySync(client, messageStore, v, logger)

		case *events.MediaRetry:
			// Complete only an internally pending receive-side media recovery.
			// This event is never exposed as a REST or MCP write capability.
			mediaRetryRequests.handle(v)

		case *events.Connected:
			logger.Infof("Connected to WhatsApp")

		case *events.LoggedOut:
			logger.Warnf("Device logged out, please scan QR code to log in again")
		}
	})

	// Create channel to track connection success
	connected := make(chan bool, 1)

	// Connect to WhatsApp
	if client.Store.ID == nil {
		// No ID stored, this is a new client, need to pair with phone
		qrChan, _ := client.GetQRChannel(context.Background())
		err = client.Connect()
		if err != nil {
			logger.Errorf("Failed to connect: %v", err)
			return
		}

		// Print QR code for pairing with phone
		for evt := range qrChan {
			if evt.Event == "code" {
				fmt.Println("\nScan this QR code with your WhatsApp app:")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
			} else if evt.Event == "success" {
				connected <- true
				break
			}
		}

		// Wait for connection
		select {
		case <-connected:
			fmt.Println("\nSuccessfully connected and authenticated!")
		case <-time.After(3 * time.Minute):
			logger.Errorf("Timeout waiting for QR code scan")
			return
		}
	} else {
		// Already logged in, just connect
		err = client.Connect()
		if err != nil {
			logger.Errorf("Failed to connect: %v", err)
			return
		}
		connected <- true
	}

	// Wait a moment for connection to stabilize
	time.Sleep(2 * time.Second)

	if !client.IsConnected() {
		logger.Errorf("Failed to establish stable connection")
		return
	}

	fmt.Println("\n✓ Connected to WhatsApp! Type 'help' for commands.")

	// Start REST API server
	server, serverFailures := startRESTServer(client, messageStore, 8080)

	watchdogContext, cancelWatchdog := context.WithCancel(context.Background())
	watchdogTicker := time.NewTicker(connectionCheckInterval)
	watchdogFailures := make(chan error, 1)
	go func() {
		if err := monitorConnection(
			watchdogContext,
			client,
			watchdogTicker.C,
			connectionUnhealthyLimit,
		); err != nil {
			watchdogFailures <- err
		}
	}()

	// Create a channel to keep the main goroutine alive
	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(exitChan)

	fmt.Println("REST server is running. Press Ctrl+C to disconnect and exit.")

	// Wait for a graceful signal or a runtime failure that systemd must restart.
	runtimeErr := waitForRuntimeExit(exitChan, permanentDisconnects, watchdogFailures, serverFailures)
	cancelWatchdog()
	watchdogTicker.Stop()

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	if err := server.Shutdown(shutdownContext); err != nil {
		logger.Warnf("REST API shutdown failed: %v", err)
	}
	cancelShutdown()

	fmt.Println("Disconnecting...")
	// Disconnect client
	client.Disconnect()

	if runtimeErr != nil {
		logger.Errorf("Bridge stopped after runtime failure: %v", runtimeErr)
		if err := messageStore.Close(); err != nil {
			logger.Warnf("Failed to close message store: %v", err)
		}
		os.Exit(1)
	}
}

// GetChatName determines the appropriate name for a chat based on JID and other info
func GetChatName(client *whatsmeow.Client, messageStore *MessageStore, jid types.JID, chatJID string, conversation interface{}, sender string, logger waLog.Logger) string {
	// First, check if chat already exists in database with a name
	var existingName string
	err := messageStore.db.QueryRow("SELECT name FROM chats WHERE jid = ?", chatJID).Scan(&existingName)
	if err == nil && existingName != "" {
		// Chat exists with a name, use that
		logger.Infof("Using existing chat name for %s: %s", chatJID, existingName)
		return existingName
	}

	// Need to determine chat name
	var name string

	if jid.Server == "g.us" {
		// This is a group chat
		logger.Infof("Getting name for group: %s", chatJID)

		// Use conversation data if provided (from history sync)
		if conversation != nil {
			// Extract name from conversation if available
			// This uses type assertions to handle different possible types
			var displayName, convName *string
			// Try to extract the fields we care about regardless of the exact type
			v := reflect.ValueOf(conversation)
			if v.Kind() == reflect.Ptr && !v.IsNil() {
				v = v.Elem()

				// Try to find DisplayName field
				if displayNameField := v.FieldByName("DisplayName"); displayNameField.IsValid() && displayNameField.Kind() == reflect.Ptr && !displayNameField.IsNil() {
					dn := displayNameField.Elem().String()
					displayName = &dn
				}

				// Try to find Name field
				if nameField := v.FieldByName("Name"); nameField.IsValid() && nameField.Kind() == reflect.Ptr && !nameField.IsNil() {
					n := nameField.Elem().String()
					convName = &n
				}
			}

			// Use the name we found
			if displayName != nil && *displayName != "" {
				name = *displayName
			} else if convName != nil && *convName != "" {
				name = *convName
			}
		}

		// If we didn't get a name, try group info
		if name == "" {
			groupInfo, err := client.GetGroupInfo(context.Background(), jid)
			if err == nil && groupInfo.Name != "" {
				name = groupInfo.Name
			} else {
				// Fallback name for groups
				name = fmt.Sprintf("Group %s", jid.User)
			}
		}

		logger.Infof("Using group name: %s", name)
	} else {
		// This is an individual contact
		logger.Infof("Getting name for contact: %s", chatJID)

		// Just use contact info (full name)
		contact, err := client.Store.Contacts.GetContact(context.Background(), jid)
		if err == nil && contact.FullName != "" {
			name = contact.FullName
		} else if sender != "" {
			// Fallback to sender
			name = sender
		} else {
			// Last fallback to JID
			name = jid.User
		}

		logger.Infof("Using contact name: %s", name)
	}

	return name
}

// Handle history sync events
func handleHistorySync(client *whatsmeow.Client, messageStore *MessageStore, historySync *events.HistorySync, logger waLog.Logger) {
	fmt.Printf("Received history sync event with %d conversations\n", len(historySync.Data.Conversations))

	syncedCount := 0
	for _, conversation := range historySync.Data.Conversations {
		// Parse JID from the conversation
		if conversation.ID == nil {
			continue
		}

		chatJID := *conversation.ID

		// Try to parse the JID
		jid, err := types.ParseJID(chatJID)
		if err != nil {
			logger.Warnf("Failed to parse JID %s: %v", chatJID, err)
			continue
		}

		// Get appropriate chat name by passing the history sync conversation directly
		name := GetChatName(client, messageStore, jid, chatJID, conversation, "", logger)

		// Process messages
		messages := conversation.Messages
		if len(messages) > 0 {
			// Update chat with latest message timestamp
			latestMsg := messages[0]
			if latestMsg == nil || latestMsg.Message == nil {
				continue
			}

			// Get timestamp from message info
			timestamp := time.Time{}
			if ts := latestMsg.Message.GetMessageTimestamp(); ts != 0 {
				timestamp = time.Unix(int64(ts), 0)
			} else {
				continue
			}

			messageStore.StoreChat(chatJID, name, timestamp)

			// Store messages
			for _, msg := range messages {
				if msg == nil || msg.Message == nil {
					continue
				}

				// Extract text content
				var content string
				if msg.Message.Message != nil {
					if conv := msg.Message.Message.GetConversation(); conv != "" {
						content = conv
					} else if ext := msg.Message.Message.GetExtendedTextMessage(); ext != nil {
						content = ext.GetText()
					}
				}

				// Extract media info
				var mediaType, filename, url string
				var mediaKey, fileSHA256, fileEncSHA256 []byte
				var fileLength uint64

				if msg.Message.Message != nil {
					mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength = extractMediaInfo(msg.Message.Message)
				}

				// Log the message content for debugging
				logger.Infof("Message content: %v, Media Type: %v", content, mediaType)

				// Skip messages with no content and no media
				if content == "" && mediaType == "" {
					continue
				}

				// Determine sender
				var sender, senderJID string
				isFromMe := false
				if msg.Message.Key != nil {
					if msg.Message.Key.FromMe != nil {
						isFromMe = *msg.Message.Key.FromMe
					}
					if msg.Message.Key.Participant != nil && *msg.Message.Key.Participant != "" {
						sender = *msg.Message.Key.Participant
						senderJID = *msg.Message.Key.Participant
					} else if isFromMe {
						sender = client.Store.ID.User
						senderJID = client.Store.ID.ToNonAD().String()
					} else {
						sender = jid.User
						senderJID = jid.String()
					}
				} else {
					sender = jid.User
					senderJID = jid.String()
				}

				// Store message
				msgID := ""
				if msg.Message.Key != nil && msg.Message.Key.ID != nil {
					msgID = *msg.Message.Key.ID
				}

				// Get message timestamp
				timestamp := time.Time{}
				if ts := msg.Message.GetMessageTimestamp(); ts != 0 {
					timestamp = time.Unix(int64(ts), 0)
				} else {
					continue
				}

				err = messageStore.StoreMessage(
					msgID,
					chatJID,
					sender,
					senderJID,
					content,
					timestamp,
					isFromMe,
					mediaType,
					filename,
					url,
					mediaKey,
					fileSHA256,
					fileEncSHA256,
					fileLength,
				)
				if err != nil {
					logger.Warnf("Failed to store history message: %v", err)
				} else {
					syncedCount++
					// Log successful message storage
					if mediaType != "" {
						logger.Infof("Stored message: [%s] %s -> %s: [%s: %s] %s",
							timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, mediaType, filename, content)
					} else {
						logger.Infof("Stored message: [%s] %s -> %s: %s",
							timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, content)
					}
				}
			}
		}
	}

	fmt.Printf("History sync complete. Stored %d messages.\n", syncedCount)
}
