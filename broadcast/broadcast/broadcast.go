package broadcast

import (
	"encoding/json"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastNode struct {
	maelstrom.Node
	messages   map[int]struct{}
	neighbours []string
	m          sync.RWMutex
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

	for _, nb := range n.neighbours {
		n.Send(
			nb,
			map[string]any{
				"type":    "broadcast",
				"message": request.Message,
			},
		)
	}

	return BroadcastResponse{
		Type: "broadcast_ok",
	}, nil
}
