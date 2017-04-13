# pubsubscheduler
Provides future scheduling built over Google's pubsub.

[Pub/Sub](https://cloud.google.com/pubsub/) is a fully managed realtime messaging service. This package provides functionality above google's pubsub of future scheduling of messages.
Useful for cases when you have to build a functionality which happens in future. For example - send a reminder for an event which is going to happen in future.

How it works?
-------------

The package exposes a simple `Publish` method which takes a 'time of message' as an input. If the time is within the specified window the message is published immediately, else a new message is created which wraps the original message and pushed over a new topic named 'scheduler'.
The package also provides a worker which listens on this topic and receives the message as soon as it is published. The worker stores the message in a datastore and publishes accordingly when the correct time has arrived.

The worker currently stores the messages in a boltdb datastore. Any datastore which can implement the `DataStore` interface will work. Using boltdb has several advantages though:
1. No extra dependency of maintaining a separate datastore
2. The operations needed are very nicely handled by boltdb as it stores the key in a lexicographic order and we need the keys in time sorted order. (So, that we simple store key the time in RFC3339 format as the key)
3. Workers will work even in a distributed environment. Every worker will have their own subset of data and will schedule them accordingly.


Steps:
-------------

1. Set the "GOOGLE_APPLICATION_CREDENTIALS" environment variable. 
2. Start the worker
```
go build && ./worker -projectId <your_project_id>
```
3. Use the publisher from your application like this:
```
func main() {
  pubsubscheduler.InitPubSub(<projectId>, time.Second)
}

func someFunc() {
  _, err := pubsubscheduler.Publish(<your_topic>, time.Now().Add(time.Hour), data_bytes, attributesMap)
  ...
}
```


License
-------

Licensed under the MIT License. See the LICENSE file for more information.
