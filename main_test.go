package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

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
