package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()

	kv := maelstrom.NewSeqKV(node)

	timeout := 1 * time.Second

	key := "count"

	node.Handle("init", func(msg maelstrom.Message) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		key += node.ID()

		err := kv.Write(ctx, key, 0)
		if err != nil {
			return err
		}

		return nil
	})

	var lock sync.Mutex

	node.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "add_ok"

		delta := int(body["delta"].(float64))

		lock.Lock()
		value, err := kv.ReadInt(context.Background(), key)
		if err != nil {
			return err
		}

		err = kv.Write(context.Background(), key, value+delta)
		lock.Unlock()
		if err != nil {
			return err
		}

		delete(body, "delta")

		return node.Reply(msg, body)
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"

		value := 0

		for _, nodeID := range node.NodeIDs() {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			res, err := node.SyncRPC(ctx, nodeID, map[string]any{
				"type": "local",
			})
			if err != nil {
				continue
			}

			var resBody map[string]any
			if err = json.Unmarshal(res.Body, &resBody); err != nil {
				continue
			}

			value += int(resBody["value"].(float64))
		}

		body["value"] = value

		return node.Reply(msg, body)
	})

	node.Handle("local", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "local_ok"

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		lock.Lock()
		value, err := kv.ReadInt(ctx, key)
		lock.Unlock()
		if err != nil {
			return err
		}

		body["value"] = value

		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
