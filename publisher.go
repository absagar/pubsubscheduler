package pubsubscheduler

import (
	"cloud.google.com/go/pubsub"
	"encoding/json"
	"time"
	"golang.org/x/net/context"
	"os"
	"log"
)

const (
	PUBSUB_TOPIC_SCHEDULE          = "scheduler"
)

var (
	pubSubClient *pubsub.Client
	window time.Duration
)

func InitPubSub(projectId string, w time.Duration) error {
	window = w

	if gceCred := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); gceCred == "" {
		log.Fatalln("GOOGLE_APPLICATION_CREDENTIALS should be set")
	}

	var err error
	ctx := context.Background()
	pubSubClient, err = pubsub.NewClient(ctx, projectId)
	if err != nil {
		return err
	}

	pubSubClient.CreateTopic(ctx, PUBSUB_TOPIC_SCHEDULE)

	return nil
}

func Publish(topic string, when time.Time, data []byte, attr map[string]string) (*pubsub.PublishResult, error) {
	ctx := context.Background()
	now := time.Now()

	var msg pubsub.Message
	var t *pubsub.Topic
	if now.Add(window).After(when) {
		t = pubSubClient.Topic(topic)

		msg = pubsub.Message{
			Data:       data,
			Attributes: attr,
		}
	} else {
		//publish to scheduler topic
		t = pubSubClient.Topic(PUBSUB_TOPIC_SCHEDULE)
		bts, err := json.Marshal(SchedulerPayload{
			Topic: topic,
			Data:  data,
			When:  when,
			Attr:  attr,
		})
		if err != nil {
			return nil, err
		}
		msg = pubsub.Message{
			Data: bts,
		}
	}

	return t.Publish(ctx, &msg), nil
}
