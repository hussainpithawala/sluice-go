package unit

import (
	"context"
	"testing"

	"github.com/hussainpithawala/sluice-go/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSink is a minimal FlushSink for testing engine behavior.
type fakeSink struct {
	result *sink.BulkWriteResult
	err    error
}

func (f *fakeSink) BulkWrite(ctx context.Context, models []sink.WriteModel) (*sink.BulkWriteResult, error) {
	return f.result, f.err
}
func (f *fakeSink) Write(ctx context.Context, model sink.WriteModel) error { return nil }
func (f *fakeSink) Ping(ctx context.Context) error                         { return nil }
func (f *fakeSink) Close(ctx context.Context) error                        { return nil }

func TestEngine_FlushBand_AttributionGuard(t *testing.T) {
	t.Run("Unattributed error commits nothing", func(t *testing.T) {
		// A sink that returns an error with an empty CorrelationKey
		fs := &fakeSink{
			result: &sink.BulkWriteResult{
				Errors: []sink.SinkError{
					{CorrelationKey: "", Code: 500, Class: sink.ClassTransient},
				},
			},
		}

		// Verify CheckAttribution catches this
		models := []sink.WriteModel{
			{CorrelationKey: "user1"},
		}
		err := sink.CheckAttribution(models, fs.result)
		require.Error(t, err)
		assert.ErrorIs(t, err, sink.ErrUnattributedSinkError)
	})

	t.Run("IsPermanent routes correctly across sinks", func(t *testing.T) {
		tests := []struct {
			name string
			err  sink.SinkError
			want bool
		}{
			{
				name: "ClassPermanent dead-letters",
				err:  sink.SinkError{CorrelationKey: "k1", Class: sink.ClassPermanent},
				want: true,
			},
			{
				name: "ClassTransient retries",
				err:  sink.SinkError{CorrelationKey: "k2", Class: sink.ClassTransient},
				want: false,
			},
			{
				name: "ClassUnknown with 11000 dead-letters (legacy)",
				err:  sink.SinkError{CorrelationKey: "k3", Code: 11000, Class: sink.ClassUnknown},
				want: true,
			},
			{
				name: "ClassUnknown with non-11000 retries (legacy)",
				err:  sink.SinkError{CorrelationKey: "k4", Code: 500, Class: sink.ClassUnknown},
				want: false,
			},
			{
				name: "Postgres 23505 as ClassPermanent dead-letters",
				err:  sink.SinkError{CorrelationKey: "k5", Class: sink.ClassPermanent, Code: 0},
				want: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				assert.Equal(t, tt.want, tt.err.IsPermanent())
			})
		}
	})
}
