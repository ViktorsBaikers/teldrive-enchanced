package lifecycle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
)

func TestB2FixtureStorage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := NewStorage(t, []ChannelScope{{ChannelID: 101, Actors: []int64{1}}, {ChannelID: 202, Actors: []int64{1, 2}}})
	for _, m := range []Message{
		{UserID: 1, ChannelID: 101, MessageID: 7, Bytes: []byte("source"), Preserve: true},
		{UserID: 2, ChannelID: 202, MessageID: 7, Bytes: []byte("foreign"), Preserve: true},
		{UserID: 1, ChannelID: 202, MessageID: 8, Bytes: []byte("unknown"), Preserve: true},
	} {
		if err := s.Seed(m); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("independent_bytes", func(t *testing.T) {
		input := []byte("owned")
		if err := s.Seed(Message{UserID: 1, ChannelID: 202, MessageID: 9, Bytes: input}); err != nil {
			t.Fatal(err)
		}
		input[0] = '!'
		got, ok := s.Bytes(202, 9)
		if !ok || string(got) != "owned" {
			t.Fatal("seed aliases caller bytes")
		}
		got[0] = '!'
		again, _ := s.Bytes(202, 9)
		if string(again) != "owned" {
			t.Fatal("oracle aliases storage bytes")
		}
	})
	t.Run("empty_single_multi", func(t *testing.T) {
		for _, body := range [][]byte{nil, []byte("one"), []byte("two")} {
			part, err := s.Upload(ctx, telegramstore.UploadRequest{UserID: 1, ChannelID: 202, Reader: bytes.NewReader(body), Size: int64(len(body))})
			if err != nil {
				t.Fatal(err)
			}
			r, err := s.OpenRange(ctx, telegramstore.RangeRequest{UserID: 1, ChannelID: part.ChannelID, MessageID: part.MessageID, Length: -1})
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			if sha256.Sum256(got) != sha256.Sum256(body) {
				t.Fatal("byte oracle mismatch")
			}
		}
	})
	t.Run("copy_and_exact_delete", func(t *testing.T) {
		part, err := s.CopyPart(ctx, 1, 101, 7, 202)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := s.Bytes(202, part.MessageID)
		if !ok || string(got) != "source" {
			t.Fatal("copy did not retain source bytes")
		}
		for range 2 {
			if err := s.DeleteMessages(ctx, 1, 202, []int64{part.MessageID, 9}); err != nil {
				t.Fatal(err)
			}
		}
		if _, ok := s.Bytes(202, part.MessageID); ok {
			t.Fatal("exact delete left destination")
		}
		if err := s.CheckPreserved(); err != nil {
			t.Fatal(err)
		}
		calls := s.Deletes()
		if len(calls) != 2 || calls[0].UserID != 1 || calls[0].ChannelID != 202 || len(calls[0].MessageIDs) != 2 {
			t.Fatal("delete oracle lost actor/channel/IDs")
		}
		calls[0].MessageIDs[0] = -1
		if s.Deletes()[0].MessageIDs[0] == -1 {
			t.Fatal("delete oracle aliases log")
		}
	})
	t.Run("receipt_and_error", func(t *testing.T) {
		fault := errors.New("post-accept failure")
		s.SetSendFaults(SendFaults{UploadError: fault})
		part, err := s.Upload(ctx, telegramstore.UploadRequest{UserID: 1, ChannelID: 202, Reader: bytes.NewBufferString("accepted"), Size: 8})
		if !errors.Is(err, fault) || part.MessageID == 0 {
			t.Fatal("known receipt lost with error")
		}
		if got, ok := s.Bytes(202, part.MessageID); !ok || string(got) != "accepted" {
			t.Fatal("accepted bytes absent")
		}
		s.SetSendFaults(SendFaults{CopyError: fault, CopyWithoutReceipt: true})
		before := len(s.Snapshot())
		part, err = s.CopyPart(ctx, 1, 101, 7, 202)
		if !errors.Is(err, fault) || part.MessageID != 0 || len(s.Snapshot()) != before+1 {
			t.Fatal("unknown remote acceptance was not retained independently")
		}
		s.SetSendFaults(SendFaults{})
	})
	t.Run("barrier_cancel_release_teardown", func(t *testing.T) {
		b := NewBarrier(t, nil)
		done := make(chan error, 1)
		go func() { done <- b.Pause(ctx) }()
		if err := b.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		b.Release()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		cancelled, stop := context.WithCancel(ctx)
		stop()
		if err := NewBarrier(t, nil).Pause(cancelled); !errors.Is(err, context.Canceled) {
			t.Fatal("cancel did not release barrier")
		}
		closed := NewBarrier(t, nil)
		closed.Close()
		if err := closed.Pause(ctx); !errors.Is(err, ErrFixtureClosed) {
			t.Fatal("teardown did not release barrier")
		}
		lost := NewBarrier(t, ErrProcessLost)
		lost.Release()
		if err := lost.Pause(ctx); !errors.Is(err, ErrProcessLost) {
			t.Fatal("process loss not distinct from rollback")
		}
	})
	t.Run("delete_ack_loss", func(t *testing.T) {
		part, err := s.CopyPart(ctx, 1, 101, 7, 202)
		if err != nil {
			t.Fatal(err)
		}
		b := NewBarrier(t, ErrResponseLost)
		b.Release()
		s.SetDeleteFaults(DeleteFaults{AfterDelete: b})
		if err := s.DeleteMessages(ctx, 1, 202, []int64{part.MessageID}); !errors.Is(err, ErrResponseLost) {
			t.Fatal("lost delete ACK not surfaced")
		}
		if _, ok := s.Bytes(202, part.MessageID); ok {
			t.Fatal("delete ACK loss pretended remote rollback")
		}
		s.SetDeleteFaults(DeleteFaults{})
	})
	t.Run("real_response_truncation", func(t *testing.T) {
		b := NewBarrier(t, nil)
		b.Release()
		hooks := Hooks{UploadTerminalResponse: b}
		server := httptest.NewServer(hooks.UploadTerminalHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":{"code":"upload_parent_unavailable"}}`))
		})))
		defer server.Close()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err == nil || len(body) != 1 || resp.StatusCode != http.StatusConflict {
			t.Fatal("terminal response was not truly truncated")
		}
	})
	t.Run("channel_delete_is_destructive", func(t *testing.T) {
		other := NewStorage(t, nil)
		channel, err := other.CreateChannel(ctx, 1, "owned")
		if err != nil {
			t.Fatal(err)
		}
		part, err := other.Upload(ctx, telegramstore.UploadRequest{UserID: 1, ChannelID: channel.ID, Reader: bytes.NewBufferString("payload"), Size: 7})
		if err != nil {
			t.Fatal(err)
		}
		if err := other.DeleteChannel(ctx, 1, channel.ID); err != nil {
			t.Fatal(err)
		}
		if _, ok := other.Bytes(channel.ID, part.MessageID); ok {
			t.Fatal("whole-channel delete was a count-only fake")
		}
	})
	t.Run("cancel_after_remote_acceptance", func(t *testing.T) {
		accepted := NewBarrier(t, nil)
		s.SetSendFaults(SendFaults{AfterUpload: accepted})
		requestCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		before := len(s.Snapshot())
		go func() {
			part, err := s.Upload(requestCtx, telegramstore.UploadRequest{UserID: 1, ChannelID: 202, Reader: bytes.NewBufferString("late"), Size: 4})
			if part.MessageID == 0 {
				done <- errors.New("accepted receipt lost")
				return
			}
			done <- err
		}()
		if err := accepted.Wait(ctx); err != nil {
			stop()
			t.Fatal(err)
		}
		stop()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation did not preserve known acceptance")
			}
		case <-ctx.Done():
			t.Fatal("accepted send did not terminate")
		}
		if len(s.Snapshot()) != before+1 {
			t.Fatal("canceled remote acceptance disappeared")
		}
		s.SetSendFaults(SendFaults{})
	})
	t.Run("scope_range_and_failed_delete", func(t *testing.T) {
		if _, err := s.CopyPart(ctx, 2, 101, 7, 202); !errors.Is(err, telegramstore.ErrInvalidChannel) {
			t.Fatal("foreign actor accessed source channel")
		}
		r, err := s.OpenRange(ctx, telegramstore.RangeRequest{UserID: 1, ChannelID: 101, MessageID: 7, Offset: 1, Length: 3})
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if string(body) != "our" {
			t.Fatal("range bytes incorrect")
		}
		if _, err := s.OpenRange(ctx, telegramstore.RangeRequest{UserID: 1, ChannelID: 101, MessageID: 7, Offset: -1}); !errors.Is(err, telegramstore.ErrInvalidRequest) {
			t.Fatal("invalid range accepted")
		}
		fault := errors.New("remote delete denied")
		s.SetDeleteFaults(DeleteFaults{Error: fault})
		if err := s.DeleteMessages(ctx, 1, 101, []int64{7}); !errors.Is(err, fault) {
			t.Fatal("delete failure hidden")
		}
		if err := s.CheckPreserved(); err != nil {
			t.Fatal(err)
		}
		s.SetDeleteFaults(DeleteFaults{})
	})
	t.Run("teardown_releases_replaced_fault", func(t *testing.T) {
		other := NewStorage(t, []ChannelScope{{ChannelID: 303, Actors: []int64{1}}})
		gate := NewBarrier(t, nil)
		other.SetSendFaults(SendFaults{AfterUpload: gate})
		done := make(chan error, 1)
		go func() {
			_, err := other.Upload(ctx, telegramstore.UploadRequest{UserID: 1, ChannelID: 303, Reader: bytes.NewReader(nil), Size: 0})
			done <- err
		}()
		if err := gate.Wait(ctx); err != nil {
			other.Close()
			t.Fatal(err)
		}
		other.SetSendFaults(SendFaults{})
		other.Close()
		select {
		case err := <-done:
			if !errors.Is(err, ErrFixtureClosed) {
				t.Fatal("teardown did not stop replaced fault")
			}
		case <-ctx.Done():
			t.Fatal("teardown leaked blocked send")
		}
	})
	if err := s.CheckPreserved(); err != nil {
		t.Fatal(err)
	}
}
