package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
)

func main() {
	node := maelstrom.NewNode()

	messages := make(map[string][]any)
	commitedOffsets := make(map[string]int)
	var lock sync.Mutex

	node.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		message := body["msg"].(any)

		lock.Lock()

		if _, ok := messages[key]; !ok {
			messages[key] = []any{}
		}

		offset := len(messages[key]) - 1
		messages[key] = append(messages[key], []any{offset, message})

		lock.Unlock()

		return node.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})
	})

	node.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := body["offsets"].(map[string]any)

		msgs := make(map[string][]any)

		for key, offset := range offsets {
			if _, ok := messages[key]; !ok {
				msgs[key] = []any{}
				continue
			}

			msgs[key] = messages[key][int(offset.(float64)):]
		}

		return node.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": messages,
		})
	})

	node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := body["offsets"].(map[string]any)

		lock.Lock()
		for key, offset := range offsets {
			commitedOffsets[key] = int(offset.(float64))
		}
		lock.Unlock()

		return node.Reply(msg, map[string]any{
			"type": "commit_offsets_ok",
		})
	})

	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		keys := body["keys"].([]any)

		offsets := make(map[string]int)

		lock.Lock()
		for _, key := range keys {
			if offset, ok := commitedOffsets[key.(string)]; ok {
				offsets[key.(string)] = offset
			}
		}
		lock.Unlock()

		return node.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": offsets,
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
