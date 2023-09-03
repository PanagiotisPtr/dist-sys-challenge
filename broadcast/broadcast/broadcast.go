package broadcast

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastNode struct {
	maelstrom.Node
	messages   map[int]struct{}
	neighbours []string
	m          sync.RWMutex
	routines   sync.WaitGroup
	shutdown   chan struct{}
}

func MaelstromHandler[Request any, Response any](
	n *maelstrom.Node,
	h func(Request) (Response, error),
) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var request Request
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return err
		}
		res, err := h(request)
		if err != nil {
			return err
		}

		return n.Reply(msg, res)
	}
}

func NewBroadcastNode() *BroadcastNode {
	n := &BroadcastNode{
		Node:     *maelstrom.NewNode(),
		messages: make(map[int]struct{}),
	}

	n.Handle("topology", MaelstromHandler(&n.Node, n.Topology))
	n.Handle("read", MaelstromHandler(&n.Node, n.Read))
	n.Handle("broadcast", MaelstromHandler(&n.Node, n.Broadcast))
	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	return n
}

type TopologyRequest struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type TopologyResponse struct {
	Type string `json:"type"`
}

func (n *BroadcastNode) Topology(request TopologyRequest) (
	TopologyResponse,
	error,
) {
	n.m.Lock()
	defer n.m.Unlock()
	for _, nb := range request.Topology[n.ID()] {
		n.neighbours = append(n.neighbours, nb)
	}

	return TopologyResponse{Type: "topology_ok"}, nil
}

type ReadRequest struct {
	Type string `json:"type"`
}

type ReadResponse struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

func (n *BroadcastNode) Read(request ReadRequest) (
	ReadResponse,
	error,
) {
	n.m.RLock()
	defer n.m.RUnlock()
	ms := make([]int, len(n.messages))
	i := 0
	for m := range n.messages {
		ms[i] = m
		i++
	}

	return ReadResponse{
		Type:     "read_ok",
		Messages: ms,
	}, nil
}

type BroadcastRequest struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type BroadcastResponse struct {
	Type string `json:"type"`
}

func (n *BroadcastNode) Broadcast(request BroadcastRequest) (
	BroadcastResponse,
	error,
) {
	if _, ok := n.messages[request.Message]; ok {
		return BroadcastResponse{
			Type: "broadcast_ok",
		}, nil
	}

	n.m.Lock()
	n.messages[request.Message] = struct{}{}
	n.m.Unlock()

	go func() {
		n.routines.Add(1)
		defer n.routines.Done()
		attempts := 0
		wg := sync.WaitGroup{}
		m := sync.Mutex{}
		checks := make([]bool, len(n.neighbours))
		for attempts < 100 {
			for i, nb := range n.neighbours {
				if checks[i] {
					continue
				}
				wg.Add(1)
				go func(j int, nbg string) {
					defer wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					res, err := n.SyncRPC(
						ctx,
						nbg,
						map[string]any{
							"type":    "broadcast",
							"message": request.Message,
						},
					)
					if err == nil && res.Type() == "broadcast_ok" {
						m.Lock()
						checks[j] = true
						m.Unlock()
					}
				}(i, nb)
			}
			select {
			case <-n.shutdown:
				return
			default:
				wg.Wait()
				done := true
				for _, c := range checks {
					if !c {
						done = false
						break
					}
				}
				if done {
					break
				}
				attempts++
				time.Sleep(time.Second)
			}
		}
	}()

	return BroadcastResponse{
		Type: "broadcast_ok",
	}, nil
}

func (n *BroadcastNode) Run() error {
	defer func() {
		n.shutdown <- struct{}{}
		close(n.shutdown)

		n.routines.Wait()
	}()

	return n.Node.Run()
}
