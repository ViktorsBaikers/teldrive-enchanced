package telegramstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

type b2Runner struct {
	api   *tg.Client
	after error
}

func (r b2Runner) Run(ctx context.Context, _ int64, _ Operation, fn func(context.Context, *tg.Client) error) error {
	return errors.Join(fn(ctx, r.api), r.after)
}

type b2Bots struct{ err error }

func (b b2Bots) ChannelBots(context.Context, int64, *tg.Client) ([]tg.InputUserClass, error) {
	return []tg.InputUserClass{&tg.InputUser{UserID: 11}, &tg.InputUser{UserID: 12}}, b.err
}
func b2Reply(out bin.Decoder, value bin.Encoder) error {
	var b bin.Buffer
	if err := value.Encode(&b); err != nil {
		return err
	}
	return out.Decode(&b)
}
func b2Channel(id int64) *tg.Channel {
	return &tg.Channel{ID: id, Title: "fixture", Broadcast: true, AccessHash: 19, Photo: &tg.ChatPhotoEmpty{}}
}
func b2Updates(channel, id, size int64) *tg.Updates {
	return &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateNewChannelMessage{Message: &tg.Message{ID: int(id), PeerID: &tg.PeerChannel{ChannelID: channel}, Media: &tg.MessageMediaDocument{Document: &tg.Document{ID: 71, Size: size}}}}}}
}
func b2Factory(t *testing.T, rateLimit bool) *Factory {
	t.Helper()
	f, err := NewFactory(FactoryConfig{AppID: 1, AppHash: "synthetic", MaxRetries: 3, RateLimit: rateLimit, RateInterval: time.Millisecond, RateBurst: 1})
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func b2Storage(t *testing.T, invoke telegram.InvokeFunc, after error, options ...GotdStorageOption) *GotdStorage {
	t.Helper()
	return NewGotdStorage(b2Runner{tg.NewClient(b2Factory(t, false).wrapInvoker(1, invoke)), after}, nil, options...)
}

func TestB2StorageReceipt(t *testing.T) {
	cause := errors.New("post-send failure")
	for _, method := range []string{"upload", "copy"} {
		for _, tc := range []struct {
			name              string
			channel, id, size int64
			after             error
			want              bool
			sizeErr           bool
		}{
			{"success", 202, 8, 4, nil, true, false}, {"size_failure", 202, 8, 3, nil, true, true}, {"outer_runner", 202, 8, 4, cause, true, false}, {"wrong_destination", 303, 8, 4, nil, false, false}, {"nonpositive", 202, 0, 4, nil, false, false}, {"source_identity", 101, 7, 4, nil, false, false},
		} {
			t.Run(method+"/"+tc.name, func(t *testing.T) {
				s := b2Storage(t, func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
					switch r := in.(type) {
					case *tg.ChannelsGetChannelsRequest:
						return b2Reply(out, &tg.MessagesChats{Chats: []tg.ChatClass{b2Channel(r.ID[0].(*tg.InputChannel).ChannelID)}})
					case *tg.ChannelsGetMessagesRequest:
						return b2Reply(out, &tg.MessagesMessages{Messages: []tg.MessageClass{&tg.Message{ID: 7, PeerID: &tg.PeerChannel{ChannelID: 101}, Media: &tg.MessageMediaDocument{Document: &tg.Document{ID: 71, Size: 4}}}}})
					case *tg.UploadSaveFilePartRequest:
						return b2Reply(out, &tg.BoolTrue{})
					case *tg.MessagesSendMediaRequest:
						return b2Reply(out, b2Updates(tc.channel, tc.id, tc.size))
					default:
						return fmt.Errorf("unexpected RPC %T", in)
					}
				}, tc.after)
				var got StoredPart
				var err error
				if method == "upload" {
					got, err = s.Upload(context.Background(), UploadRequest{UserID: 1, ChannelID: 202, Name: "part", Reader: strings.NewReader("data"), Size: 4, Threads: 1})
				} else {
					got, err = s.CopyPart(context.Background(), 1, 101, 7, 202)
				}
				if tc.want {
					if got != (StoredPart{ChannelID: 202, MessageID: 8, Size: tc.size}) {
						t.Fatalf("lost receipt: %+v %v", got, err)
					}
				} else if got != (StoredPart{}) || err == nil {
					t.Fatalf("untrusted receipt: %+v %v", got, err)
				}
				if tc.after != nil && !errors.Is(err, tc.after) {
					t.Fatalf("lost runner cause: %v", err)
				}
				if tc.sizeErr && !errors.Is(err, ErrSizeMismatch) {
					t.Fatalf("lost size cause: %v", err)
				}
				if tc.want && !tc.sizeErr && tc.after == nil && err != nil {
					t.Fatal(err)
				}
			})
		}
	}
	t.Run("truthful_delete_channel", func(t *testing.T) {
		for _, failure := range []error{ErrInvalidChannel, tgerr.New(401, "AUTH_KEY_UNREGISTERED"), tgerr.New(400, "CHANNEL_PRIVATE"), tgerr.New(400, "CHAT_ADMIN_REQUIRED")} {
			deletes := 0
			s := b2Storage(t, func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
				if _, ok := in.(*tg.ChannelsDeleteChannelRequest); ok {
					deletes++
				}
				return failure
			}, nil)
			if err := s.DeleteChannel(context.Background(), 1, 202); !errors.Is(err, failure) || deletes != 0 {
				t.Fatalf("false deletion ACK: %v %d", err, deletes)
			}
			if err := s.DeleteMessages(context.Background(), 1, 202, []int64{8}); !errors.Is(err, failure) {
				t.Fatalf("false message ACK: %v", err)
			}
		}
	})
	t.Run("delete_rpc_failure", func(t *testing.T) {
		failure := tgerr.New(400, "CHAT_ADMIN_REQUIRED")
		s := b2Storage(t, func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
			if _, ok := in.(*tg.ChannelsGetChannelsRequest); ok {
				return b2Reply(out, &tg.MessagesChats{Chats: []tg.ChatClass{b2Channel(202)}})
			}
			return failure
		}, nil)
		if err := s.DeleteChannel(context.Background(), 1, 202); !errors.Is(err, failure) {
			t.Fatalf("false delete ACK: %v", err)
		}
	})
	t.Run("copy_same_channel_source_identity", func(t *testing.T) {
		s := b2Storage(t, func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
			switch in.(type) {
			case *tg.ChannelsGetChannelsRequest:
				return b2Reply(out, &tg.MessagesChats{Chats: []tg.ChatClass{b2Channel(101)}})
			case *tg.ChannelsGetMessagesRequest:
				return b2Reply(out, &tg.MessagesMessages{Messages: []tg.MessageClass{&tg.Message{ID: 7, PeerID: &tg.PeerChannel{ChannelID: 101}, Media: &tg.MessageMediaDocument{Document: &tg.Document{ID: 71, Size: 4}}}}})
			case *tg.MessagesSendMediaRequest:
				return b2Reply(out, b2Updates(101, 7, 4))
			default:
				return fmt.Errorf("unexpected %T", in)
			}
		}, nil)
		got, err := s.CopyPart(context.Background(), 1, 101, 7, 101)
		if got != (StoredPart{}) || err == nil {
			t.Fatalf("source became destination ownership: %+v %v", got, err)
		}
	})
	t.Run("explicit_delete_channel_success", func(t *testing.T) {
		deletes := 0
		s := b2Storage(t, func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
			switch in.(type) {
			case *tg.ChannelsGetChannelsRequest:
				return b2Reply(out, &tg.MessagesChats{Chats: []tg.ChatClass{b2Channel(202)}})
			case *tg.ChannelsDeleteChannelRequest:
				deletes++
				return b2Reply(out, &tg.Updates{})
			default:
				return fmt.Errorf("unexpected %T", in)
			}
		}, nil)
		if err := s.DeleteChannel(context.Background(), 1, 202); err != nil || deletes != 1 {
			t.Fatalf("explicit delete: %v %d", err, deletes)
		}
	})
	for _, tc := range []struct {
		name          string
		lookup, after error
		failAdmin     int
		unknown       bool
	}{
		{name: "create_channel_bot_lookup_retained", lookup: cause}, {name: "create_channel_bot_admin_retained", failAdmin: 1}, {name: "create_channel_outer_runner_retained", after: cause}, {name: "create_channel_unknown_result_retained", unknown: true}, {name: "create_channel_success_control"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, adminFailure := range []int{tc.failAdmin, 2} {
				if tc.failAdmin == 0 && adminFailure == 2 {
					continue
				}
				admins, deletes := 0, 0
				s := b2Storage(t, func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
					switch in.(type) {
					case *tg.ChannelsCreateChannelRequest:
						if tc.unknown {
							return b2Reply(out, &tg.Updates{})
						}
						return b2Reply(out, &tg.Updates{Chats: []tg.ChatClass{b2Channel(202)}})
					case *tg.ChannelsEditAdminRequest:
						admins++
						if tc.failAdmin != 0 && admins == adminFailure {
							return cause
						}
						return b2Reply(out, &tg.Updates{})
					case *tg.ChannelsDeleteChannelRequest:
						deletes++
						return b2Reply(out, &tg.Updates{})
					default:
						return fmt.Errorf("unexpected %T", in)
					}
				}, tc.after, WithBotProvider(b2Bots{tc.lookup}))
				got, err := s.CreateChannel(context.Background(), 1, "fixture")
				if deletes != 0 {
					t.Fatal("automatic channel rollback")
				}
				if tc.unknown {
					if err == nil || got.ID != 0 {
						t.Fatalf("invented identity: %+v %v", got, err)
					}
					var creation *ChannelCreationError
					if errors.As(err, &creation) {
						t.Fatal("unknown result became ownership")
					}
					continue
				}
				if got.ID != 202 {
					t.Fatalf("lost channel: %+v %v", got, err)
				}
				if tc.lookup != nil || tc.after != nil || tc.failAdmin != 0 {
					var creation *ChannelCreationError
					if !errors.Is(err, cause) || !errors.As(err, &creation) || creation.UserID != 1 || creation.ChannelID != 202 || creation.Err == nil {
						t.Fatalf("lost allocation error: %+v %v", creation, err)
					}
				} else if err != nil || admins != 2 {
					t.Fatalf("provisioning success: %v %d", err, admins)
				}
			}
		})
	}
}

func TestB2DestructiveInvocationBudget(t *testing.T) {
	for _, failure := range []error{errors.New("connection reset by peer"), tgerr.New(-503, "Timeout"), tgerr.New(420, "FLOOD_WAIT_1")} {
		t.Run(failure.Error(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				s := b2Storage(t, func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
					switch in.(type) {
					case *tg.ChannelsGetChannelsRequest:
						return b2Reply(out, &tg.MessagesChats{Chats: []tg.ChatClass{b2Channel(202)}})
					case *tg.ChannelsDeleteMessagesRequest:
						calls++
						return failure
					default:
						return fmt.Errorf("unexpected %T", in)
					}
				}, nil)
				if err := s.DeleteMessages(context.Background(), 1, 202, []int64{8}); !errors.Is(err, failure) || calls != 1 {
					t.Fatalf("destructive replay: %v %d", err, calls)
				}
			})
		})
	}
	for _, test := range []struct {
		name    string
		failure error
	}{
		{"channel_transient", errors.New("connection reset by peer")},
		{"channel_timeout", tgerr.New(-503, "Timeout")},
		{"channel_flood_wait", tgerr.New(420, "FLOOD_WAIT_1")},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				storage := b2Storage(t, func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
					switch input.(type) {
					case *tg.ChannelsGetChannelsRequest:
						return b2Reply(output, &tg.MessagesChats{Chats: []tg.ChatClass{b2Channel(202)}})
					case *tg.ChannelsDeleteChannelRequest:
						calls++
						return test.failure
					default:
						return fmt.Errorf("unexpected %T", input)
					}
				}, nil)
				if err := storage.DeleteChannel(context.Background(), 1, 202); !errors.Is(err, test.failure) || calls != 1 {
					t.Fatalf("channel delete replay: %v, calls=%d", err, calls)
				}
			})
		})
	}
	for _, name := range []string{"empty", "dedup_chunks", "invalid", "cancelled", "cancel_mid_chunks", "partial_failure"} {
		t.Run(name, func(t *testing.T) {
			seen := map[int]int{}
			batches := 0
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ids := []int64{}
			if name != "empty" {
				for i := 1; i <= 205; i++ {
					ids = append(ids, int64(i), int64(i))
				}
			}
			if name == "invalid" {
				ids = append(ids, 0)
			}
			if name == "cancelled" {
				cancel()
			}
			s := b2Storage(t, func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
				switch r := in.(type) {
				case *tg.ChannelsGetChannelsRequest:
					return b2Reply(out, &tg.MessagesChats{Chats: []tg.ChatClass{b2Channel(202)}})
				case *tg.ChannelsDeleteMessagesRequest:
					batches++
					for _, id := range r.ID {
						seen[id]++
					}
					if name == "cancel_mid_chunks" {
						cancel()
					}
					if name == "partial_failure" && batches == 2 {
						return errors.New("connection reset by peer")
					}
					return b2Reply(out, &tg.MessagesAffectedMessages{})
				default:
					return fmt.Errorf("unexpected %T", in)
				}
			}, nil)
			err := s.DeleteMessages(ctx, 1, 202, ids)
			if name == "empty" || name == "invalid" || name == "cancelled" {
				if batches != 0 {
					t.Fatal("invalid/empty/cancelled input sent")
				}
				if name != "empty" && err == nil {
					t.Fatal("missing failure")
				}
			} else {
				for id, count := range seen {
					if count != 1 {
						t.Fatalf("replayed %d: %d", id, count)
					}
				}
				if name == "dedup_chunks" && (err != nil || len(seen) != 205 || batches != 3) {
					t.Fatalf("chunks: %d %d %v", batches, len(seen), err)
				}
				if name == "partial_failure" && (err == nil || batches != 2) {
					t.Fatalf("partial: %d %v", batches, err)
				}
			}
			if name == "cancel_mid_chunks" && (batches != 1 || !errors.Is(err, context.Canceled)) {
				t.Fatalf("sent after cancellation: %d %v", batches, err)
			}
		})
	}
	t.Run("normal_retry_and_limiter", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			calls := 0
			start := time.Now()
			api := tg.NewClient(b2Factory(t, true).wrapInvoker(1, telegram.InvokeFunc(func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
				if _, ok := in.(*tg.UploadGetFileRequest); !ok {
					t.Fatalf("unexpected %T", in)
				}
				calls++
				if calls == 1 {
					return errors.New("connection reset by peer")
				}
				if calls == 2 {
					return tgerr.New(420, "FLOOD_WAIT_1")
				}
				return b2Reply(out, &tg.UploadFile{Type: &tg.StorageFileUnknown{}})
			})))
			for range 2 {
				if _, err := api.UploadGetFile(context.Background(), &tg.UploadGetFileRequest{}); err != nil {
					t.Fatal(err)
				}
			}
			if calls != 4 || time.Since(start) < 1501*time.Millisecond {
				t.Fatalf("ordinary retry/limiter: %d %v", calls, time.Since(start))
			}
		})
	})
}
