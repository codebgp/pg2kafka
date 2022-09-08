package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/codebgp/pg2kafka/eventqueue"
	"github.com/codebgp/pg2kafka/healthcheck"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var (
	topicNamespace string
	topicVersion   string
	// Version the app version set at link time
	Version string
	// L is the package exposed logger
	L *zap.Logger
)

// Producer is the minimal required interface pg2kafka requires to produce
// events to a kafka topic.
type Producer interface {
	Close()
	Flush(int) int

	Produce(*kafka.Message, chan kafka.Event) error
}

func main() {
	var err error
	L, err = zap.NewProduction()
	if os.Getenv("ENV") != "production" {
		L, err = zap.NewDevelopment()
	}
	if err != nil {
		L.Fatal("Error setting up logger", zap.Error(err))
	}

	// done channel can signal termination to go routines reading it
	var (
		done   = make(chan struct{})
		errors = make(chan error, 1)
	)

	// Setup healthcheck provider
	_ = healthcheck.EnableProvider(L, healthcheck.NeverFailHealthCheck, done)

	databaseURL := os.Getenv("DATABASE_URL")
	topicVersion = os.Getenv("TOPIC_VERSION")
	topicNamespace = parseTopicNamespace(os.Getenv("TOPIC_NAMESPACE"), parseDatabaseName(databaseURL))

	eq, err := eventqueue.New(&eventqueue.DBConfig{DatabaseURL: databaseURL})
	if err != nil {
		L.Fatal("Error opening event queue db connection", zap.Error(err))
	}
	defer func() {
		if cerr := eq.Close(); cerr != nil {
			L.Fatal("Error closing event queue db connection", zap.Error(cerr))
		}
	}()

	if os.Getenv("PERFORM_MIGRATIONS") == "true" {
		if cerr := eq.ConfigureOutboundEventQueueAndTriggers("./sql"); cerr != nil {
			L.Fatal("Error configuring outbound_event_queue and triggers", zap.Error(cerr))
		}
	} else {
		L.Info("Not performing database migrations due to missing `PERFORM_MIGRATIONS`.")
	}

	producer := setupProducer()
	defer producer.Close()
	defer producer.Flush(1000)

	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			L.Info("Received postgres error notify", zap.Any("event", pqNotifyEventToString(ev)), zap.Error(err))
			return
		}
		L.Info("Received postgres notify event", zap.Any("event", pqNotifyEventToString(ev)))
	}
	listener := pq.NewListener(databaseURL, 10*time.Second, time.Minute, reportProblem)
	if err := listener.Listen("outbound_event_queue"); err != nil {
		L.Fatal("Error listening to pg", zap.Error(err))
	}
	defer func() {
		if cerr := listener.Close(); cerr != nil {
			L.Error("Error closing listener", zap.Error(cerr))
		}
		L.Info(fmt.Sprintf("pg2kafka[commit:%s] stopped", Version))
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	go func() {
		<-signals
		close(done)
	}()

	L.Info(fmt.Sprintf("pg2kafka[commit:%s] started", Version))
	// Process any events in the queue
	// TODO: the process cannot be aborted while processing the accummulated queue.
	// This can cause ungraceful termination of the process.
	err = processQueue(producer, eq)
	if err != nil {
		errors <- err
	}

	L.Info(fmt.Sprintf("pg2kafka[commit:%s] is now listening to notifications", Version))
	waitForNotification(listener, producer, eq, done, errors)
}

// ProcessEvents queries the database for unprocessed events and produces them
// to kafka.
func ProcessEvents(p Producer, eq *eventqueue.Queue) error {
	events, err := eq.FetchUnprocessedRecords()
	if err != nil {
		L.Error("Failed to fetch unprocessed records", zap.Error(err))
		return err
	}

	err = produceMessages(p, events, eq)
	if err != nil {
		return err
	}

	return nil
}

func processQueue(p Producer, eq *eventqueue.Queue) error {
	L.Debug("Starting queue processing")
	pageCount, err := eq.UnprocessedEventPagesCount()
	if err != nil {
		L.Error("Failed to fetch unprocessed record pages", zap.Error(err))
		return err
	}

	for i := 0; i <= pageCount; i++ {
		err = ProcessEvents(p, eq)
		if err != nil {
			return err
		}
	}

	L.Debug("Finished queue processing")
	return nil
}

func waitForNotification(
	l *pq.Listener,
	p Producer,
	eq *eventqueue.Queue,
	done chan struct{},
	errors chan error,
) {
	for {
		select {
		case <-l.Notify:
			emptyNotificationsChannel(l.Notify, 100*time.Millisecond)
			err := processQueue(p, eq)
			if err != nil && len(errors) == 0 {
				errors <- err
			}
		case <-errors:
			emptyNotificationsChannel(l.Notify, 100*time.Millisecond)
			err := processQueue(p, eq)
			if err != nil && len(errors) == 0 {
				errors <- err
			}
		case <-time.After(90 * time.Second):
			go func() {
				err := l.Ping()
				if err != nil {
					L.Info("Failed pinging listener", zap.Error(err))
					return
				}
				count, err := eq.CountUnprocessedEvents()
				if err != nil {
					L.Info("Failed fetching count of unprocessed events", zap.Error(err))
					return
				}
				L.Info("Unprocessed events in queue", zap.Any("count", count))
			}()
		case <-done:
			return
		}
	}
}

func produceMessages(p Producer, events []*eventqueue.Event, eq *eventqueue.Queue) error {
	deliveryChan := make(chan kafka.Event)
	for _, event := range events {
		msg, err := json.Marshal(event)
		if err != nil {
			L.Error("Error serialising event", zap.Error(err))
			return err
		}

		topic := topicName(topicNamespace, event.TableName, topicVersion)
		message := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny, // nolint: gotype
			},
			Value:     msg,
			Key:       event.ExternalID,
			Timestamp: event.CreatedAt,
		}
		if os.Getenv("DRY_RUN") != "" {
			L.Info("Would produce message", zap.Any("message", message))
		} else {
			err = p.Produce(message, deliveryChan)
			if err != nil {
				L.Error("Failed to produce", zap.Error(err), zap.String("topic", topic))
				return err
			}
			e := <-deliveryChan

			result := e.(*kafka.Message)
			if result.TopicPartition.Error != nil {
				L.Error("Delivery failed", zap.Error(result.TopicPartition.Error), zap.String("topic", topic))
				return err
			}
		}
		err = eq.DeleteEvent(event.ID)
		if err != nil {
			L.Error("Error deleting record", zap.Error(err), zap.Int("id", event.ID))
			return err
		}
	}

	return nil
}

func setupProducer() Producer {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		panic("missing KAFKA_BROKER environment")
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = os.Getenv("HOSTNAME")
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"client.id":         fmt.Sprintf("pg2kafka-%s", hostname),
		"bootstrap.servers": broker,
		"partitioner":       "murmur2",
		"compression.codec": "snappy",
	})
	if err != nil {
		panic(errors.Wrap(err, "failed to setup producer"))
	}

	return p
}

func topicName(topicNamespace, tableName, topicVersion string) string {
	name := fmt.Sprintf("pg2kafka.%v.%v", topicNamespace, tableName)
	if topicVersion != "" {
		name = fmt.Sprintf("%s.%s", name, topicVersion)
	}
	return name
}

func parseDatabaseName(conninfo string) string {
	dbURL, err := url.Parse(conninfo)
	if err != nil {
		L.Fatal("Error parsing db connection string", zap.Error(err))
	}
	return strings.TrimPrefix(dbURL.Path, "/")
}

func parseTopicNamespace(topicNamespace string, databaseName string) string {
	s := databaseName
	if topicNamespace != "" {
		s = topicNamespace + "." + s
	}

	return s
}

func pqNotifyEventToString(ev pq.ListenerEventType) string {
	switch ev {
	case pq.ListenerEventConnected:
		return "connected"
	case pq.ListenerEventDisconnected:
		return "disconnected"
	case pq.ListenerEventConnectionAttemptFailed:
		return "connection attempt failed"
	case pq.ListenerEventReconnected:
		return "reconnected"
	default:
		return "unknown"
	}
}

func emptyNotificationsChannel(nc <-chan *pq.Notification, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	for {
		select {
		case _, open := <-nc:
			if len(nc) == 0 || !open {
				return
			}
		case <-timer.C:
			return
		}
	}
}
