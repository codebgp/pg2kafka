//go:build integrationtests
// +build integrationtests

package main

import (
	"database/sql"
	"os"
	"testing"

	"github.com/buger/jsonparser"
	"github.com/codebgp/pg2kafka/eventqueue"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestFetchUnprocessedRecords(t *testing.T) {
	db, eq, cleanup := setup(t)
	defer cleanup()

	events := []*eventqueue.Event{
		{
			ExternalID:    []byte("fefc72b4-d8df-4039-9fb9-bfcb18066a2b"),
			TableName:     "users",
			Statement:     "UPDATE",
			State:         []byte(`{ "email": "jurre@blendle.com" }`),
			ChangedFields: []string{"email"},
		},
		{
			ExternalID:    []byte("fefc72b4-d8df-4039-9fb9-bfcb18066a2b"),
			TableName:     "users",
			Statement:     "UPDATE",
			State:         []byte(`{ "email": "jurres@blendle.com" }`),
			ChangedFields: []string{},
		},
		{
			ExternalID: nil,
			TableName:  "users",
			Statement:  "CREATE",
			State:      []byte(`{ "email": "bart@simpsons.com" }`),
		},
		{
			ExternalID:    nil,
			TableName:     "users",
			Statement:     "UPDATE",
			State:         []byte(`{ "email": "bartman@simpsons.com" }`),
			ChangedFields: []string{"email"},
		},
	}
	if err := insert(db, events); err != nil {
		t.Fatalf("Error inserting events: %v", err)
	}

	p := &mockProducer{
		messages: make([]*kafka.Message, 0),
	}

	ProcessEvents(p, eq)

	expected := 4
	actual := len(p.messages)
	if actual != expected {
		t.Fatalf("Unexpected number of messages produced. Expected %d, got %d", expected, actual)
	}

	msg := p.messages[0]
	email, err := jsonparser.GetString(msg.Value, "state", "email")
	if err != nil {
		t.Fatal(err)
	}

	if email != "jurre@blendle.com" {
		t.Errorf("Data did not match. Expected %v, got %v", "jurre@blendle.com", email)
	}

	externalID, err := jsonparser.GetString(msg.Value, "external_id")
	if err != nil {
		t.Fatal(err)
	}

	if externalID != "fefc72b4-d8df-4039-9fb9-bfcb18066a2b" {
		t.Errorf("Expected %v, got %v", "fefc72b4-d8df-4039-9fb9-bfcb18066a2b", externalID)
	}

	msg = p.messages[3]
	email, err = jsonparser.GetString(msg.Value, "state", "email")
	if err != nil {
		t.Fatal(err)
	}

	if email != "bartman@simpsons.com" {
		t.Errorf("Data did not match. Expected %v, got %v", "bartman@simpsons.com", email)
	}

	if len(msg.Key) != 0 {
		t.Errorf("Expected empty key, got %v", msg.Key)
	}

	for _, msg := range p.messages {
		statement, _ := jsonparser.GetString(msg.Value, "statement")
		switch statement {
		case "CREATE":
			_, valueType, _, _ := jsonparser.Get(msg.Value, "changed_fields")
			assert.Equal(t, valueType, jsonparser.Null, "Expected empty for create events")
		case "UPDATE":
			_, err = jsonparser.ArrayEach(msg.Value, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
				assert.Equal(t, value, []byte("email"), "Expected changed fields for update events")
			}, "changed_fields")
			assert.Nil(t, err, "Expected not nil changed_fields")
		default:
			t.Fatal("Expected only insert and update statement events")
		}

	}
}

// Helpers

func setup(t *testing.T) (*sql.DB, *eventqueue.Queue, func()) {
	t.Helper()
	topicNamespace = "users"
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	eq := eventqueue.NewWithDB(db)
	if err := eq.ConfigureOutboundEventQueueAndTriggers("./sql"); err != nil {
		t.Fatal(err)
	}

	return db, eq, func() {
		_, err := db.Exec("DELETE FROM pg2kafka.outbound_event_queue")
		if err != nil {
			t.Fatalf("failed to clear table: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Error closing db: %v", err)
		}
	}
}

func insert(db *sql.DB, events []*eventqueue.Event) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	statement, err := tx.Prepare(`
		INSERT INTO pg2kafka.outbound_event_queue (external_id, table_name, statement, state, changed_fields, processed)
		VALUES ($1, $2, $3, $4, $5, $6)
	`)
	if err != nil {
		if txerr := tx.Rollback(); txerr != nil {
			return txerr
		}
		return err
	}

	for _, e := range events {
		_, serr := statement.Exec(e.ExternalID, e.TableName, e.Statement, e.State, pq.Array(e.ChangedFields), e.Processed)
		if serr != nil {
			if txerr := tx.Rollback(); err != nil {
				return txerr
			}
			return serr
		}
	}
	return tx.Commit()
}

type mockProducer struct {
	messages []*kafka.Message
}

func (p *mockProducer) Close() {
}
func (p *mockProducer) Flush(timeout int) int {
	return 0
}
func (p *mockProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	p.messages = append(p.messages, msg)
	go func() {
		deliveryChan <- msg
	}()
	return nil
}
