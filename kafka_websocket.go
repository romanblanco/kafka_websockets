package main

import (
  "fmt"
  "time"
  "github.com/Shopify/sarama"
  "github.com/pote/philote-go"
)

func main() {

  for {
    kafkaConfig := sarama.NewConfig()
    kafkaConfig.Consumer.Return.Errors = true
    kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
    kafkaConfig.Producer.Retry.Max = 5
    kafkaConfig.Producer.Return.Successes = true

    brokers := []string{"localhost:9092"}

    // create a new consumer
    master, err := sarama.NewConsumer(brokers, kafkaConfig)
    if err != nil {
      panic(err)
    }
    fmt.Printf("kafka consumer created: %T\n", master)

    defer func() {
      if err := master.Close(); err != nil {
        panic(err)
      }
    }()

    // create a new producer
    producer, err := sarama.NewSyncProducer(brokers, kafkaConfig)
    if err != nil {
      panic(err)
    }
    fmt.Printf("kafka producer created: %T\n", producer)

    defer func() {
      if err := producer.Close(); err != nil {
        panic(err)
      }
    }()

    // set a kafka topic to follow
    topic := "test"

    // produce a kafka message
    msg := &sarama.ProducerMessage{
      Topic: topic,
      Value: sarama.StringEncoder("Something Cool"),
    }
    partition, offset, err := producer.SendMessage(msg)
    if err != nil {
      panic(err)
    }
    fmt.Printf("kafka message produced: p: %v, o: %v\n", partition, offset)

    // consume a kafka message
    consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetOldest)
    if err != nil {
      panic(err)
    }
    select {
      case err := <-consumer.Errors():
        fmt.Println(err)
      case msg := <-consumer.Messages():
        fmt.Println("kafka message consumed: ", string(msg.Key), string(msg.Value))
    }

    // create a websocket token
    token, _ := philote.NewToken(
      "roman",
      []string{"read-write-channel"},
      []string{"read-write-channel"},
    )
    fmt.Printf("philote token created: %T\n", token)

    fmt.Println(token)

    // create a websocket client
    c1, _ := philote.NewClient("ws://localhost:6380", token)
    fmt.Printf("philote client 1 created: %T\n", c1)

    c2, _ := philote.NewClient("ws://localhost:6380", token)
    fmt.Printf("philote client 2 created: %T\n", c2)

    // publish a websocket message
    go func() {
      time.Sleep(2*time.Second)
      c1.Publish(
        &philote.Message{
          Channel: "read-write-channel",
          Data: "My message to the world",
        })
      if err != nil {
        panic(err)
      }
      fmt.Println("c1: philote message sent")
    }()

    // receive a websocket message
    message, err := c2.Receive()
    if err != nil {
      panic(err)
    }
    fmt.Printf("c2: philote message received: %v\n", message.Data)
  }
}
