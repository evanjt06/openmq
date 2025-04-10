package main

import (
	"fmt"
	"math/rand"
	openmq "openmq/internal"
	"time"
)

func fakeEmailService(payload string) error {
	// Simulate 30% chance of failure
	if rand.Intn(10) < 3 {
		return fmt.Errorf("email service failed to send: %s", payload)
	}
	fmt.Println("✅ Email sent:", payload)
	return nil
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// 1. Create a persistent QueueManager
	qm := openmq.NewQueueManager(false, "")

	// 2. Enqueue some emails
	fmt.Println("🔁 Enqueueing 5 email jobs...")
	for i := 1; i <= 5; i++ {
		payload := fmt.Sprintf("email #%d - Welcome to Scholarhive!", i)
		qm.Enqueue("email", payload, nil)
	}

	// 3. Worker loop to process messages
	for {
		msg, err := qm.Dequeue("email")
		if err != nil {
			fmt.Println("📭 No more messages in queue.")
			break
		}

		fmt.Println("📥 Processing:", msg.Payload)

		// Simulate email service
		err = fakeEmailService(msg.Payload)
		if err != nil {
			fmt.Println("❌ Nacking message:", msg.ID)
			qm.Nack("email", msg.ID)
		} else {
			fmt.Println("✅ Acking message:", msg.ID)
			qm.Ack("email", msg.ID)
		}

		time.Sleep(500 * time.Millisecond)
	}

	// 4. Show DLQ
	fmt.Println("\n🪦 DEAD LETTER QUEUE:")
	dlq := qm.GetDLQ("email")
	for _, msg := range dlq {
		fmt.Printf("💀 Failed Message (retries=%d): %s\n", msg.RetryCount, msg.Payload)
	}
}
