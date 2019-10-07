package main

import (
  "fmt"
  "time"
  "github.com/Shopify/sarama"
  "github.com/pote/philote-go"
)

func main() {
  // set a kafka topic to follow
  topic := "test"

  kafkaConfig := sarama.NewConfig()
  kafkaConfig.Consumer.Return.Errors = true
  kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
  kafkaConfig.Producer.Retry.Max = 5
  kafkaConfig.Producer.Return.Successes = true

  brokers := []string{"localhost:9092"}

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

  for {
    fmt.Println("********************************")

    // produce a kafka message
    go func() {
      kafka_message := time.Now().String() + ": " + "Kafka says hi!"
      msg := &sarama.ProducerMessage{
        Topic: topic,
        Value: sarama.StringEncoder(kafka_message),
      }
      partition, offset, err := producer.SendMessage(msg)
      if err != nil {
        panic(err)
      }
      fmt.Printf("kafka >: p: %v, o: %v, m: %v\n",
        partition,
        offset,
        kafka_message)
    }()

    // publish a websocket message
    go func() {
      websocket_message := time.Now().String() + ": " + "Websockets say hi!"
      time.Sleep(2*time.Second)
      c1.Publish(
        &philote.Message{
          Channel: "read-write-channel",
          Data: websocket_message,
        })
      if err != nil {
        panic(err)
      }
      fmt.Println("websocket(c1) >: ", websocket_message)
    }()

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

    // consume a kafka message
    consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetNewest)
    if err != nil {
      panic(err)
    }
    select {
      case err := <-consumer.Errors():
        fmt.Println(err)
      case msg := <-consumer.Messages():
        fmt.Println("kafka <: ", string(msg.Key), string(msg.Value))
        // redirect the kafka message to websocket channel
        time.Sleep(2*time.Second)
        c1.Publish(
          &philote.Message{
            Channel: "read-write-channel",
            Data: "K: " + string(msg.Value),
          })
        if err != nil {
          panic(err)
        }
        fmt.Println("kafka > websocket(c1): ", string(msg.Value))
    }

    // receive a websocket message
    message, err := c2.Receive()
    if err != nil {
      panic(err)
    }
    fmt.Printf("websocket(c2) <: %v\n", message.Data)

    fmt.Println("********************************")
  }
}
