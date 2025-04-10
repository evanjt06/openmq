// openmq_v1.go
package openmq

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Message struct {
	ID         string
	Payload    string
	Timestamp  time.Time
	TTL        *time.Duration
	RetryCount int
}

type TopicQueue struct {
	Messages []*Message
	InFlight map[string]*Message
	DLQ      []*Message
	Mu       sync.Mutex
}

type QueueManager struct {
	Topics     map[string]*TopicQueue
	Persistent bool
	LogPath    string
	LogMu      sync.Mutex
}

func NewQueueManager(persistent bool, logPath string) *QueueManager {
	return &QueueManager{
		Topics:     make(map[string]*TopicQueue),
		Persistent: persistent,
		LogPath:    logPath,
	}
}

func (qm *QueueManager) getTopic(topic string) *TopicQueue {
	if _, exists := qm.Topics[topic]; !exists {
		qm.Topics[topic] = &TopicQueue{
			Messages: []*Message{},
			InFlight: make(map[string]*Message),
			DLQ:      []*Message{},
		}
	}
	return qm.Topics[topic]
}

func (qm *QueueManager) Enqueue(topic string, payload string, ttl *time.Duration) string {
	topicQueue := qm.getTopic(topic)
	topicQueue.Mu.Lock()
	defer topicQueue.Mu.Unlock()

	msg := &Message{
		ID:        uuid.NewString(),
		Payload:   payload,
		Timestamp: time.Now(),
		TTL:       ttl,
	}
	topicQueue.Messages = append(topicQueue.Messages, msg)

	if qm.Persistent {
		qm.appendToLog("ENQUEUE", topic, msg)
	}
	return msg.ID
}

func (qm *QueueManager) Dequeue(topic string) (*Message, error) {
	topicQueue := qm.getTopic(topic)
	topicQueue.Mu.Lock()
	defer topicQueue.Mu.Unlock()

	if len(topicQueue.Messages) == 0 {
		return nil, fmt.Errorf("no messages in topic")
	}

	msg := topicQueue.Messages[0]
	topicQueue.Messages = topicQueue.Messages[1:]
	topicQueue.InFlight[msg.ID] = msg
	return msg, nil
}

func (qm *QueueManager) Ack(topic string, msgID string) error {
	topicQueue := qm.getTopic(topic)
	topicQueue.Mu.Lock()
	defer topicQueue.Mu.Unlock()

	if _, ok := topicQueue.InFlight[msgID]; !ok {
		return fmt.Errorf("message ID not found in in-flight")
	}
	delete(topicQueue.InFlight, msgID)
	return nil
}

func (qm *QueueManager) Nack(topic string, msgID string) error {
	topicQueue := qm.getTopic(topic)
	topicQueue.Mu.Lock()
	defer topicQueue.Mu.Unlock()

	msg, ok := topicQueue.InFlight[msgID]
	if !ok {
		return fmt.Errorf("message ID not found in in-flight")
	}

	delete(topicQueue.InFlight, msgID)
	msg.RetryCount++
	if msg.RetryCount >= 3 {
		topicQueue.DLQ = append(topicQueue.DLQ, msg)
	} else {
		topicQueue.Messages = append([]*Message{msg}, topicQueue.Messages...)
	}
	return nil
}

func (qm *QueueManager) GetDLQ(topic string) []*Message {
	topicQueue := qm.getTopic(topic)
	return topicQueue.DLQ
}

func (qm *QueueManager) appendToLog(op, topic string, msg *Message) {
	qm.LogMu.Lock()
	defer qm.LogMu.Unlock()

	file, err := os.OpenFile(qm.LogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	logEntry := map[string]interface{}{
		"op":        op,
		"topic":     topic,
		"message":   msg,
		"timestamp": time.Now(),
	}
	data, _ := json.Marshal(logEntry)
	file.WriteString(string(data) + "\n")
}

func (qm *QueueManager) ReplayLog(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		var logEntry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &logEntry); err != nil {
			continue
		}
		topic := logEntry["topic"].(string)
		msgMap := logEntry["message"].(map[string]interface{})

		msg := &Message{
			ID:        msgMap["ID"].(string),
			Payload:   msgMap["Payload"].(string),
			Timestamp: time.Now(),
		}
		qm.getTopic(topic).Messages = append(qm.getTopic(topic).Messages, msg)
	}
	return scanner.Err()
}
