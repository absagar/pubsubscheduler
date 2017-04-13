package pubsubscheduler

import "time"

type SchedulerPayload struct {
	When  time.Time
	Topic string
	Data  []byte
	Attr  map[string]string
}
