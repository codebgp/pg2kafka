package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

var parseTopicNamespacetests = []struct {
	in1, in2, out string
}{
	{"", "", ""},
	{"", "world", "world"},
	{"hello", "", "hello."},
	{"hello", "world", "hello.world"},
}

func TestParseTopicNamespace(t *testing.T) {
	for _, tt := range parseTopicNamespacetests {
		t.Run(tt.out, func(t *testing.T) {
			actual := parseTopicNamespace(tt.in1, tt.in2)

			if actual != tt.out {
				t.Errorf("parseTopicNamespace(%q, %q) => %v, want: %v", tt.in1, tt.in2, actual, tt.out)
			}
		})
	}
}

func TestTopicName(t *testing.T) {
	t.Parallel()

	type testInput struct {
		topicNamespace string
		topicVersion   string
		tableName      string
	}
	type test struct {
		name  string
		input testInput
		want  string
	}

	tests := []test{
		{
			name:  "default topic name",
			input: testInput{topicNamespace: "testNs", tableName: "testName"},
			want:  "pg2kafka.testNs.testName",
		},
		{
			name:  "topic name with version",
			input: testInput{topicNamespace: "testNs", tableName: "testName", topicVersion: "v1.0.0"},
			want:  "pg2kafka.testNs.testName.v1.0.0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := topicName(tc.input.topicNamespace, tc.input.tableName, tc.input.topicVersion)
			assert.Equal(t, tc.want, got)
		})
	}
}

func Test_drainNotificationsChannel(t *testing.T) {
	t.Parallel()

	type args struct {
		nc      chan *pq.Notification
		timeout time.Duration
	}
	tests := []struct {
		name        string
		args        args
		prepareFunc func(*args)
		assertFunc  func(*testing.T, *args)
	}{
		{
			name: "many elements in channel",
			args: args{
				nc:      make(chan *pq.Notification, 10),
				timeout: 10 * time.Millisecond,
			},
			prepareFunc: func(a *args) {
				for i := 0; i < 5; i++ {
					a.nc <- &pq.Notification{}
				}
				close(a.nc)
			},
			assertFunc: func(t *testing.T, a *args) {
				assert.Len(t, a.nc, 0)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareFunc(&tt.args)

			drainNotificationChannel(tt.args.nc, tt.args.timeout)

			tt.assertFunc(t, &tt.args)
		})
	}
}
