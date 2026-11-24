// Package lifecycle contains synthetic B2 fixtures, never a Telegram transport.
package lifecycle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"sort"
	"sync"
	"testing"

	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
)

const maxFixtureBytes = 128 << 20

// ChannelScope is remote access, not proof of ownership or deletion eligibility.
type ChannelScope struct {
	ChannelID int64
	Actors    []int64
}

// Message is independent fixture provenance. Preserve marks source, foreign,
// unknown or currently referenced bytes that the tested policy must not delete.
type Message struct {
	UserID    int64
	ChannelID int64
	MessageID int64
	Bytes     []byte
	Preserve  bool
}

type messageKey struct{ channel, message int64 }

// MessageSnapshot never aliases stored bytes and survives runtime replacement.
type MessageSnapshot struct {
	UserID    int64
	ChannelID int64
	MessageID int64
	Bytes     []byte
	SHA256    [32]byte
}

// DeleteCall records the actual actor, physical channel and exact requested IDs,
// including failed calls and remote-success/acknowledgement-loss calls.
type DeleteCall struct {
	UserID       int64
	ChannelID    int64
	MessageIDs   []int64
	DeletedIDs   []int64
	PreservedIDs []int64
	Err          error
}

// SendFaults affect configured upload/copy boundaries until replaced. Configuration
// is snapshotted at invocation so barriers never hold the storage mutex.
type SendFaults struct {
	BeforeUpload         *Barrier
	AfterUpload          *Barrier
	UploadError          error
	UploadWithoutReceipt bool
	BeforeCopy           *Barrier
	AfterCopy            *Barrier
	CopyError            error
	CopyWithoutReceipt   bool
}

type DeleteFaults struct {
	BeforeDelete *Barrier
	AfterDelete  *Barrier
	Error        error
}

// Storage deliberately performs requested remote deletes, even if unsafe.
// CheckPreserved and Deletes expose policy bugs rather than masking them with
// fake reference/ownership enforcement unavailable at the real Telegram seam.
type Storage struct {
	mu           sync.Mutex
	channels     map[int64]map[int64]bool
	messages     map[messageKey]Message
	preserved    map[messageKey]MessageSnapshot
	nextID       map[int64]int64
	nextChannel  int64
	byteCount    int
	deletes      []DeleteCall
	sendFaults   SendFaults
	deleteFaults DeleteFaults
	barriers     []*Barrier
	closed       bool
}

var _ telegramstore.Storage = (*Storage)(nil)
var _ telegramstore.MetadataReader = (*Storage)(nil)

func NewStorage(t testing.TB, scopes []ChannelScope) *Storage {
	t.Helper()
	s := &Storage{channels: make(map[int64]map[int64]bool), messages: make(map[messageKey]Message),
		preserved: make(map[messageKey]MessageSnapshot), nextID: make(map[int64]int64), nextChannel: 10000}
	for _, scope := range scopes {
		if scope.ChannelID == 0 || len(scope.Actors) == 0 || s.channels[scope.ChannelID] != nil {
			t.Fatal("invalid synthetic channel scope")
		}
		actors := make(map[int64]bool)
		for _, actor := range scope.Actors {
			if actor <= 0 {
				t.Fatal("invalid synthetic actor")
			}
			actors[actor] = true
		}
		s.channels[scope.ChannelID] = actors
	}
	t.Cleanup(func() {
		if err := s.CheckPreserved(); err != nil {
			t.Error(err)
		}
		s.Close()
	})
	return s
}

func (s *Storage) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	barriers := s.barriers
	s.mu.Unlock()
	for _, b := range barriers {
		b.Close()
	}
}

func (s *Storage) SetSendFaults(f SendFaults) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendFaults = f
	s.trackBarriers(f.BeforeUpload, f.AfterUpload, f.BeforeCopy, f.AfterCopy)
}
func (s *Storage) SetDeleteFaults(f DeleteFaults) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteFaults = f
	s.trackBarriers(f.BeforeDelete, f.AfterDelete)
}

func (s *Storage) trackBarriers(barriers ...*Barrier) {
	for _, b := range barriers {
		if b == nil {
			continue
		}
		if s.closed {
			b.Close()
		} else {
			s.barriers = append(s.barriers, b)
		}
	}
}

func (s *Storage) allowed(userID, channelID int64) error {
	if s.closed {
		return ErrFixtureClosed
	}
	if userID <= 0 || channelID == 0 || !s.channels[channelID][userID] {
		return telegramstore.ErrInvalidChannel
	}
	return nil
}

func (s *Storage) Seed(m Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrFixtureClosed
	}
	if m.UserID <= 0 || m.ChannelID == 0 || m.MessageID <= 0 || s.channels[m.ChannelID] == nil {
		return telegramstore.ErrInvalidRequest
	}
	key := messageKey{m.ChannelID, m.MessageID}
	if _, exists := s.messages[key]; exists {
		return errors.New("duplicate synthetic physical message")
	}
	if len(s.messages) >= 4096 || len(m.Bytes) > maxFixtureBytes-s.byteCount {
		return errors.New("synthetic byte/message budget exceeded")
	}
	m.Bytes = bytes.Clone(m.Bytes)
	s.messages[key] = m
	s.byteCount += len(m.Bytes)
	if m.MessageID > s.nextID[m.ChannelID] {
		s.nextID[m.ChannelID] = m.MessageID
	}
	if m.Preserve {
		s.preserved[key] = snapshot(m)
	}
	return nil
}

func snapshot(m Message) MessageSnapshot {
	return MessageSnapshot{UserID: m.UserID, ChannelID: m.ChannelID, MessageID: m.MessageID,
		Bytes: bytes.Clone(m.Bytes), SHA256: sha256.Sum256(m.Bytes)}
}

func (s *Storage) Bytes(channelID, messageID int64) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.messages[messageKey{channelID, messageID}]
	return bytes.Clone(m.Bytes), ok
}

func (s *Storage) Snapshot() []MessageSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]MessageSnapshot, 0, len(s.messages))
	for _, m := range s.messages {
		result = append(result, snapshot(m))
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ChannelID != result[j].ChannelID {
			return result[i].ChannelID < result[j].ChannelID
		}
		return result[i].MessageID < result[j].MessageID
	})
	return result
}

func (s *Storage) CheckPreserved() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, expected := range s.preserved {
		m, ok := s.messages[key]
		if !ok || m.UserID != expected.UserID || sha256.Sum256(m.Bytes) != expected.SHA256 || !bytes.Equal(m.Bytes, expected.Bytes) {
			return errors.New("preserved synthetic bytes were deleted or changed")
		}
	}
	return nil
}

func (s *Storage) Deletes() []DeleteCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	calls := make([]DeleteCall, len(s.deletes))
	for i, call := range s.deletes {
		calls[i] = call
		calls[i].MessageIDs = append([]int64(nil), call.MessageIDs...)
		calls[i].DeletedIDs = append([]int64(nil), call.DeletedIDs...)
		calls[i].PreservedIDs = append([]int64(nil), call.PreservedIDs...)
	}
	return calls
}

func pause(ctx context.Context, b *Barrier) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b != nil {
		return b.Pause(ctx)
	}
	return nil
}

func (s *Storage) Upload(ctx context.Context, request telegramstore.UploadRequest) (telegramstore.StoredPart, error) {
	s.mu.Lock()
	err, faults := s.allowed(request.UserID, request.ChannelID), s.sendFaults
	s.mu.Unlock()
	if err != nil {
		return telegramstore.StoredPart{}, err
	}
	if request.Reader == nil || request.Size < 0 || request.Size > maxFixtureBytes {
		return telegramstore.StoredPart{}, telegramstore.ErrInvalidRequest
	}
	if err := pause(ctx, faults.BeforeUpload); err != nil {
		return telegramstore.StoredPart{}, err
	}
	// Requests use synthetic in-memory readers; no background goroutine can leak
	// when a cancellation/fault fires after remote acceptance.
	payload, err := io.ReadAll(io.LimitReader(request.Reader, request.Size+1))
	if err != nil {
		return telegramstore.StoredPart{}, err
	}
	if int64(len(payload)) != request.Size {
		return telegramstore.StoredPart{}, telegramstore.ErrSizeMismatch
	}
	s.mu.Lock()
	part, err := s.accept(request.UserID, request.ChannelID, payload)
	s.mu.Unlock()
	if err != nil {
		return telegramstore.StoredPart{}, err
	}
	err = errors.Join(faults.UploadError, pause(ctx, faults.AfterUpload))
	if faults.UploadWithoutReceipt {
		if err == nil {
			err = ErrResponseLost
		}
		return telegramstore.StoredPart{}, err
	}
	return part, err
}

// accept runs only under mu; each physical channel owns its own message sequence.
func (s *Storage) accept(userID, channelID int64, payload []byte) (telegramstore.StoredPart, error) {
	if err := s.allowed(userID, channelID); err != nil {
		return telegramstore.StoredPart{}, err
	}
	if len(s.messages) >= 4096 || len(payload) > maxFixtureBytes-s.byteCount {
		return telegramstore.StoredPart{}, errors.New("synthetic byte/message budget exceeded")
	}
	if s.nextID[channelID] == int64(^uint64(0)>>1) {
		return telegramstore.StoredPart{}, telegramstore.ErrInvalidRequest
	}
	s.nextID[channelID]++
	id := s.nextID[channelID]
	s.messages[messageKey{channelID, id}] = Message{UserID: userID, ChannelID: channelID, MessageID: id, Bytes: bytes.Clone(payload)}
	s.byteCount += len(payload)
	return telegramstore.StoredPart{ChannelID: channelID, MessageID: id, Size: int64(len(payload))}, nil
}

func (s *Storage) CopyPart(ctx context.Context, userID, sourceChannelID, sourceMessageID, destinationChannelID int64) (telegramstore.StoredPart, error) {
	s.mu.Lock()
	faults := s.sendFaults
	err := s.allowed(userID, sourceChannelID)
	if err == nil {
		err = s.allowed(userID, destinationChannelID)
	}
	s.mu.Unlock()
	if err != nil {
		return telegramstore.StoredPart{}, err
	}
	if err := pause(ctx, faults.BeforeCopy); err != nil {
		return telegramstore.StoredPart{}, err
	}
	s.mu.Lock()
	if err := s.allowed(userID, sourceChannelID); err != nil {
		s.mu.Unlock()
		return telegramstore.StoredPart{}, err
	}
	m, ok := s.messages[messageKey{sourceChannelID, sourceMessageID}]
	if !ok {
		s.mu.Unlock()
		return telegramstore.StoredPart{}, telegramstore.ErrMessageNotFound
	}
	part, err := s.accept(userID, destinationChannelID, m.Bytes)
	s.mu.Unlock()
	if err != nil {
		return telegramstore.StoredPart{}, err
	}
	err = errors.Join(faults.CopyError, pause(ctx, faults.AfterCopy))
	if faults.CopyWithoutReceipt {
		if err == nil {
			err = ErrResponseLost
		}
		return telegramstore.StoredPart{}, err
	}
	return part, err
}

func (s *Storage) Metadata(ctx context.Context, request telegramstore.MetadataRequest) (telegramstore.StoredPart, error) {
	if err := ctx.Err(); err != nil {
		return telegramstore.StoredPart{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.allowed(request.UserID, request.ChannelID); err != nil {
		return telegramstore.StoredPart{}, err
	}
	m, ok := s.messages[messageKey{request.ChannelID, request.MessageID}]
	if !ok {
		return telegramstore.StoredPart{}, telegramstore.ErrMessageNotFound
	}
	return telegramstore.StoredPart{ChannelID: m.ChannelID, MessageID: m.MessageID, Size: int64(len(m.Bytes))}, nil
}

func (s *Storage) OpenRange(ctx context.Context, request telegramstore.RangeRequest) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.allowed(request.UserID, request.ChannelID); err != nil {
		return nil, err
	}
	m, ok := s.messages[messageKey{request.ChannelID, request.MessageID}]
	if !ok {
		return nil, telegramstore.ErrMessageNotFound
	}
	if request.Offset < 0 || request.Offset > int64(len(m.Bytes)) || request.Length < -1 {
		return nil, telegramstore.ErrInvalidRequest
	}
	end := int64(len(m.Bytes))
	if request.Length >= 0 && request.Length < end-request.Offset {
		end = request.Offset + request.Length
	}
	return io.NopCloser(bytes.NewReader(bytes.Clone(m.Bytes[request.Offset:end]))), nil
}

func (s *Storage) DeleteMessages(ctx context.Context, userID, channelID int64, ids []int64) error {
	s.mu.Lock()
	faults := s.deleteFaults
	s.mu.Unlock()
	err := pause(ctx, faults.BeforeDelete)
	s.mu.Lock()
	if err == nil {
		err = s.allowed(userID, channelID)
	}
	if len(ids) == 0 {
		err = telegramstore.ErrInvalidRequest
	}
	seen := make(map[int64]bool, len(ids))
	for _, id := range ids {
		if id <= 0 || seen[id] {
			err = telegramstore.ErrInvalidRequest
		}
		seen[id] = true
	}
	if err == nil {
		err = faults.Error
	}
	call := DeleteCall{UserID: userID, ChannelID: channelID, MessageIDs: append([]int64(nil), ids...), Err: err}
	if err == nil {
		for _, id := range ids {
			key := messageKey{channelID, id}
			if _, protected := s.preserved[key]; protected {
				call.PreservedIDs = append(call.PreservedIDs, id)
			}
			if m, exists := s.messages[key]; exists {
				s.byteCount -= len(m.Bytes)
				delete(s.messages, key)
				call.DeletedIDs = append(call.DeletedIDs, id)
			}
		}
	}
	index := len(s.deletes)
	s.deletes = append(s.deletes, call)
	s.mu.Unlock()
	if err == nil {
		err = pause(ctx, faults.AfterDelete)
	}
	s.mu.Lock()
	s.deletes[index].Err = err
	s.mu.Unlock()
	return err
}

func (s *Storage) CreateChannel(ctx context.Context, userID int64, name string) (telegramstore.Channel, error) {
	if err := ctx.Err(); err != nil {
		return telegramstore.Channel{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return telegramstore.Channel{}, ErrFixtureClosed
	}
	if userID <= 0 || name == "" || len(s.channels) >= 256 {
		return telegramstore.Channel{}, telegramstore.ErrInvalidRequest
	}
	for {
		s.nextChannel++
		if s.channels[s.nextChannel] == nil {
			break
		}
	}
	s.channels[s.nextChannel] = map[int64]bool{userID: true}
	return telegramstore.Channel{ID: s.nextChannel, Name: name}, nil
}

func (s *Storage) DeleteChannel(ctx context.Context, userID, channelID int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.allowed(userID, channelID); err != nil {
		return err
	}
	for key, m := range s.messages {
		if key.channel == channelID {
			s.byteCount -= len(m.Bytes)
			delete(s.messages, key)
		}
	}
	delete(s.channels, channelID)
	return nil
}
