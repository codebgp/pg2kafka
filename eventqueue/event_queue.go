package eventqueue

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"io/ioutil"
	"math"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

const (
	selectUnprocessedEventsQuery = `
		SELECT id, uuid, external_id, table_name, statement, changed_fields, state, created_at
		FROM pg2kafka.outbound_event_queue
		ORDER BY id ASC
		LIMIT 1000
	`

	deleteEventQuery = `
		DELETE FROM pg2kafka.outbound_event_queue
		WHERE id = $1
	`

	countUnprocessedEventsQuery = `
		SELECT count(*) AS count
		FROM pg2kafka.outbound_event_queue
	`
)

// ByteString is a special type of byte array with implemented interfaces to
// convert from and to JSON and SQL values.
type ByteString []byte

// Event represents the queued event in the database
type Event struct {
	ID            int             `json:"-"`
	UUID          string          `json:"uuid"`
	ExternalID    ByteString      `json:"external_id"`
	TableName     string          `json:"-"`
	Statement     string          `json:"statement"`
	ChangedFields []string        `json:"changed_fields"`
	State         json.RawMessage `json:"state"`
	CreatedAt     time.Time       `json:"created_at"`
	Processed     bool            `json:"-"`
}

// Queue represents the queue of snapshot/create/update/delete events stored in
// the database.
type Queue struct {
	db *sql.DB
}

// New creates a new Queue, connected to the given database URL.
func New(conninfo string) (*Queue, error) {
	db, err := sql.Open("postgres", conninfo)
	if err != nil {
		return nil, err
	}

	return &Queue{db: db}, nil
}

// NewWithDB creates a new Queue with the given database.
func NewWithDB(db *sql.DB) *Queue {
	return &Queue{db: db}
}

// FetchUnprocessedRecords fetches a page (up to 1000) of events that have not
// been marked as processed yet.
func (eq *Queue) FetchUnprocessedRecords() ([]*Event, error) {
	rows, err := eq.db.Query(selectUnprocessedEventsQuery)
	if err != nil {
		return nil, err
	}

	messages := []*Event{}
	for rows.Next() {
		msg := &Event{}
		err = rows.Scan(
			&msg.ID,
			&msg.UUID,
			&msg.ExternalID,
			&msg.TableName,
			&msg.Statement,
			pq.Array(&msg.ChangedFields),
			&msg.State,
			&msg.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}

	if cerr := rows.Close(); cerr != nil {
		return nil, cerr
	}
	return messages, nil
}

// UnprocessedEventPagesCount returns how many "pages" of events there are
// queued in the database. Currently page-size is hard-coded to 1000 events per
// page.
func (eq *Queue) UnprocessedEventPagesCount() (int, error) {
	count, err := eq.CountUnprocessedEvents()
	if err != nil {
		return 0, err
	}
	limit := 1000
	return int(math.Ceil(float64(count) / float64(limit))), nil
}

// CountUnprocessedEvents queries and returns the amount of unprocessed events.
func (eq *Queue) CountUnprocessedEvents() (int, error) {
	count := 0
	err := eq.db.QueryRow(countUnprocessedEventsQuery).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// DeleteEvent deletes an event.
func (eq *Queue) DeleteEvent(eventID int) error {
	_, err := eq.db.Exec(deleteEventQuery, eventID)
	return err
}

// Close closes the Queue's database connection.
func (eq *Queue) Close() error {
	return eq.db.Close()
}

// ConfigureOutboundEventQueueAndTriggers will set up a new schema 'pg2kafka', with
// an 'outbound_event_queue' table that is used to store events, and all the
// triggers necessary to snapshot and start tracking changes for a given table.
func (eq *Queue) ConfigureOutboundEventQueueAndTriggers(path string) error {
	migration, err := ioutil.ReadFile(path + "/migrations.sql")
	if err != nil {
		return errors.Wrap(err, "error reading migration")
	}

	_, err = eq.db.Exec(string(migration))
	if err != nil {
		return errors.Wrap(err, "failed to create table")
	}

	functions, err := ioutil.ReadFile(path + "/triggers.sql")
	if err != nil {
		return errors.Wrap(err, "Error loading functions")
	}

	_, err = eq.db.Exec(string(functions))
	if err != nil {
		return errors.Wrap(err, "Error creating triggers")
	}

	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (b *ByteString) MarshalJSON() ([]byte, error) {
	if *b == nil {
		return []byte("null"), nil
	}

	return append(append([]byte(`"`), *b...), byte('"')), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (b *ByteString) UnmarshalJSON(d []byte) error {
	var s string
	err := json.Unmarshal(d, &s)
	*b = ByteString(s)
	return err
}

// Value implements the driver.Valuer interface.
func (b *ByteString) Value() (driver.Value, error) {
	return string(*b), nil
}

// Scan implements the sql.Scanner interface.
func (b *ByteString) Scan(val interface{}) error {
	switch v := val.(type) {
	case nil:
		*b = nil
	case string:
		*b = []byte(v)
	case []byte:
		*b = v
	default:
		return errors.New("unable to convert value to ByteString")
	}

	return nil
}
