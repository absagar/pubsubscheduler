package main

import (
	"os"
	"log"
	"cloud.google.com/go/pubsub"
	"time"
	"golang.org/x/net/context"
	"github.com/absagar/pubsubscheduler"
	"encoding/json"
	"flag"
)

var (
	subName string
	checkFrequency time.Duration
	projectId string
)

func main() {
	flag.StringVar(&subName, "subscriptionName", "sub-scheduler", "name of subscription for the scheduler worker")
	flag.DurationVar(&checkFrequency, "frequency", 5 * time.Second, "frequency of the worker to check for jobs to run")
	flag.StringVar(&projectId, "projectId", "", "google project id")
	flag.Parse()

	if projectId == "" {
		log.Fatalln("Empty project id passed via flag")
	}

	gceCred := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if gceCred == "" {
		log.Fatalln("Incomplete environment", gceCred)
	}

	pubSubClient, err := pubsub.NewClient(context.Background(), projectId)
	if err != nil {
		log.Fatalln(err)
	}

	run(pubSubClient)
}

func run(client *pubsub.Client) error {
	ctx := context.Background()
	topic, _ := client.CreateTopic(ctx, pubsubscheduler.PUBSUB_TOPIC_SCHEDULE)
	sub, _ := client.CreateSubscription(ctx, subName, topic, 300*time.Second, nil)

	db, err := getBoltdbStore()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	go startPublisher(db)

	//sub := client.Subscription(subName)
	//ctx := context.Background()
	return sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		//fmt.Println("Debug", "scheduler received")
		if err := saveInDatastore(m.Data, db); err != nil {
			log.Println(err)
			return
		}

		m.Ack() // Acknowledge that we've consumed the message.
	})
}

func saveInDatastore(bts []byte, db DataStore) error {
	var payload pubsubscheduler.SchedulerPayload
	if err := json.Unmarshal(bts, &payload); err != nil {
		return err
	}

	return db.Set(payload.Topic, payload.When, bts)
}

func startPublisher(db DataStore) error {
	err := pubsubscheduler.InitPubSub(projectId, checkFrequency)
	if err != nil {
		log.Fatalln(err)
	}
	for {
		earliestTime, _ := publishOld(db)
		earliest := earliestTime.Sub(time.Now())
		if !earliestTime.IsZero() && earliest < checkFrequency {
			time.Sleep(earliest)
		} else {
			time.Sleep(checkFrequency)
		}
	}
	return nil
}

func publishOld(db DataStore) (time.Time, error) {
	keys, allBts, err := db.GetBefore(time.Now())
	if err != nil {
		return time.Time{}, err
	}
	for i, v := range allBts {
		//fmt.Println(time.Now(), "got from db")
		var payload pubsubscheduler.SchedulerPayload
		if err := json.Unmarshal(v, &payload); err != nil {
			log.Println("Error in publish:", err, v)
			continue
		}

		_, err = pubsubscheduler.Publish(payload.Topic, time.Now(), payload.Data, payload.Attr)
		if err != nil {
			log.Println("Error in publish:", err, v)
			continue
		}
		err = db.Delete(keys[i])
		if err != nil {
			log.Println("Error in publish:", err, v)
			continue
		}
	}

	return db.GetLatest(checkFrequency)
}


