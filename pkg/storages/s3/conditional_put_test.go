package s3

import (
	"errors"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

func TestIsAwsPreconditionFailed(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "412 request failure",
			err:  awserr.NewRequestFailure(awserr.New("PreconditionFailed", "exists", nil), http.StatusPreconditionFailed, "req-1"),
			want: true,
		},
		{
			name: "409 conditional conflict",
			err:  awserr.NewRequestFailure(awserr.New("ConditionalRequestConflict", "conflict", nil), http.StatusConflict, "req-2"),
			want: true,
		},
		{
			name: "precondition code without request failure",
			err:  awserr.New("PreconditionFailed", "exists", nil),
			want: true,
		},
		{
			name: "unrelated 500",
			err:  awserr.NewRequestFailure(awserr.New("InternalError", "boom", nil), http.StatusInternalServerError, "req-3"),
			want: false,
		},
		{
			name: "plain error",
			err:  errors.New("some network error"),
			want: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isAwsPreconditionFailed(c.err); got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}
