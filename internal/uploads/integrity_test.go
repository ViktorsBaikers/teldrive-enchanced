package uploads

import (
	"errors"
	"math"
	"testing"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
)

func TestB1UploadGeometry(t *testing.T) {
	for _, test := range []struct {
		name                   string
		preference, size, want int64
		invalid                bool
	}{
		{name: "omitted", want: defaultPartSize},
		{name: "one MiB", preference: 1 << 20, want: treehash.BlockSize},
		{name: "below block", preference: treehash.BlockSize - 1, want: treehash.BlockSize},
		{name: "aligned", preference: treehash.BlockSize, want: treehash.BlockSize},
		{name: "above block", preference: treehash.BlockSize + 1, want: 2 * treehash.BlockSize},
		{name: "negative preference", preference: -1, invalid: true},
		{name: "rounding overflow", preference: math.MaxInt64, invalid: true},
		{name: "part count overflow", preference: treehash.BlockSize, size: int64(math.MaxInt32)*treehash.BlockSize + 1, invalid: true},
		{name: "largest part count", preference: treehash.BlockSize, size: int64(math.MaxInt32) * treehash.BlockSize, want: treehash.BlockSize},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := negotiatePartSize(test.preference, test.size)
			if test.invalid {
				if !errors.Is(err, ErrInvalidInput) {
					t.Fatalf("geometry = %d, %v", got, err)
				}
			} else if err != nil || got != test.want {
				t.Fatalf("geometry = %d, %v; want %d", got, err, test.want)
			}
		})
	}
	if got := expectedPartCount(math.MaxInt64, treehash.BlockSize); got != math.MaxInt64/treehash.BlockSize+1 {
		t.Fatalf("overflow-safe count = %d", got)
	}
	for _, session := range []*sqlcgen.UploadSession{
		{ExpectedSize: 10, PartSize: 4},
		{ExpectedSize: -1, PartSize: math.MaxInt64 - treehash.BlockSize + 1},
	} {
		if err := validatePartShape(session, math.MaxInt32, 1); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("unsafe session shape accepted: %+v, %v", session, err)
		}
	}
}
