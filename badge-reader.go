package main

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ebfe/scard"
)

/* die on errors */
func die(err error) {
	fmt.Println(err)
	os.Exit(1)
}

/* struct for the Badge Data */
type Card struct {
	Badge string `json:"badge"`
	Time  int64  `json:"time"`
}

/* infinite loop waiting for badge to be swiped */
func waitUntilCardPresent(ctx *scard.Context, readers []string) (int, error) {
	rs := make([]scard.ReaderState, len(readers))
	for i := range rs {
		rs[i].Reader = readers[i]
		rs[i].CurrentState = scard.StateUnaware
	}
	for {
		for i := range rs {
			if rs[i].EventState&scard.StatePresent != 0 {
				return i, nil
			}
			rs[i].CurrentState = rs[i].EventState
		}
		err := ctx.GetStatusChange(rs, -1)
		if err != nil {
			return -1, err
		}
	}
}

/* infinite loop waiting for badge to be removed */
func waitUntilCardGone(ctx *scard.Context, readers []string) (int, error) {
	rs := make([]scard.ReaderState, len(readers))
	for i := range rs {
		rs[i].Reader = readers[i]
		rs[i].CurrentState = scard.StateUnaware
	}
	for {
		for i := range rs {
			err := ctx.GetStatusChange(rs, -1)
			if err != nil {
				return -1, err
			}
			cp := rs[i].EventState & scard.StatePresent
			if cp == 0 {
				return i, nil
			}
			rs[i].CurrentState = rs[i].EventState
		}
	}
}

func ReadConfig(configFile string) kafka.ConfigMap {
    kafkaConfig := make(map[string]kafka.ConfigValue)

    file, err := os.Open(configFile)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed to open file: %s", err)
        os.Exit(1)
    }
    defer file.Close()
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        line := strings.TrimSpace(scanner.Text())
        if !strings.HasPrefix(line, "#") && len(line) != 0 {
            kv := strings.Split(line, "=")
            parameter := strings.TrimSpace(kv[0])
            value := strings.TrimSpace(kv[1])
            kafkaConfig[parameter] = value
        }
    }
    if err := scanner.Err(); err != nil {
        fmt.Printf("Failed to read file: %s", err)
        os.Exit(1)
    }
    return kafkaConfig
}

/* send data to the Kafka topic */
func sendToKafka(topic string, message string) error {
	configFile := "./properties"
	conf := ReadConfig(configFile)
	p, err := kafka.NewProducer(&conf)
	if err != nil {
		return fmt.Errorf("error creating producer: %w", err)
	}
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny},
		Value: []byte(message),
	}, nil)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Produced event to topic %s: value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Value))
				}
			}
		}
	}()
	p.Flush(15 * 1000)
	p.Close()
	return nil
}

/* main function */
func main() {
	for {
		ctx, err := scard.EstablishContext()
		if err != nil {
			die(err)
		}
		defer ctx.Release()
		readers, err := ctx.ListReaders()
		if err != nil {
			die(err)
		}
		if len(readers) > 0 {
			index, err := waitUntilCardPresent(ctx, readers)
			if err != nil {
				die(err)
			}
			card, err := ctx.Connect(readers[index], scard.ShareExclusive, scard.ProtocolAny)
			if err != nil {
				die(err)
			}
			command := []byte{0xFF, 0xCA, 0x00, 0x00, 0x00}
			rsp, err := card.Transmit(command)
			if err != nil {
				die(err)
			}
			uidHex := hex.EncodeToString(rsp)
			fmt.Println("Card UID:", uidHex)
			newCard := Card{Badge: uidHex, Time: time.Now().UnixMilli()}
			message, err := json.Marshal(newCard)
			if err != nil {
				die(err)
			}
			fmt.Println("Message:", message)
			err = sendToKafka("badge-reader", string(message))
			if err != nil {
				die(err)
			}
			for {
				_, err := waitUntilCardGone(ctx, readers)
				if err != nil {
					die(err)
				}
				card.Disconnect(scard.ResetCard)
				card = nil
				ctx.Release()
				ctx = nil
				break
			}
		}
	}
}
