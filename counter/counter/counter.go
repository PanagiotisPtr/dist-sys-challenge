package counter

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const counterKey = "counter"

type CounterNode struct {
	maelstrom.Node
	neighbours []string
	mc         sync.RWMutex
	counter    int
	store      *maelstrom.KV
	routines   sync.WaitGroup
	shutdown   chan struct{}
}

type Message[T any] struct {
	Src  string `json:"src"`
	Dest string `json:"dest"`
	Body T      `json:"body"`
}

func MaelstromHandler[Request any, Response any](
	n *maelstrom.Node,
	h func(Message[Request]) (Response, error),
) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		request := Message[Request]{
			Src:  msg.Src,
			Dest: msg.Dest,
		}
		if err := json.Unmarshal(msg.Body, &request.Body); err != nil {
			return err
		}
		res, err := h(request)
		if err != nil {
			return err
		}

		return n.Reply(msg, res)
	}
}

func NewCounterNode() *CounterNode {
	n := &CounterNode{
		Node:    *maelstrom.NewNode(),
		counter: 0,
	}
	n.store = maelstrom.NewSeqKV(&n.Node)

	n.Handle("topology", MaelstromHandler(&n.Node, n.Topology))
	n.Handle("add", MaelstromHandler(&n.Node, n.Add))
	n.Handle("read", MaelstromHandler(&n.Node, n.Read))

	// poll counter to keep it in sync
	go func() {
		for {
			select {
			case <-n.shutdown:
				return
			case <-time.Tick(time.Millisecond * 100):
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				counter, err := n.store.ReadInt(ctx, counterKey)
				if err == nil {
					n.mc.Lock()
					n.counter = counter
					n.mc.Unlock()
				}
			}
		}
	}()

	return n
}

type TopologyRequest struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type TopologyResponse struct {
	Type string `json:"type"`
}

func (n *CounterNode) Topology(request Message[TopologyRequest]) (
	TopologyResponse,
	error,
) {
	for _, nb := range request.Body.Topology[n.ID()] {
		n.neighbours = append(n.neighbours, nb)
	}

	return TopologyResponse{Type: "topology_ok"}, nil
}

type AddRequest struct {
	Type  string `json:"type"`
	Delta int    `json:"delta"`
}

type AddResponse struct {
	Type string `json:"type"`
}

func (n *CounterNode) Add(request Message[AddRequest]) (AddResponse, error) {
	reqDelta := request.Body.Delta

	go func() {
		for {
			select {
			case <-n.shutdown:
				return
			case <-time.Tick(time.Millisecond * 100):
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				n.mc.RLock()
				err := n.store.CompareAndSwap(ctx, counterKey, n.counter, n.counter+reqDelta, true)
				n.mc.RUnlock()
				if err == nil {
					return
				}
			}
		}
	}()

	return AddResponse{Type: "add_ok"}, nil
}

type ReadRequest struct {
	Type string `json:"type"`
}

type ReadResponse struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

func (n *CounterNode) Read(request Message[ReadRequest]) (ReadResponse, error) {
	n.mc.RLock()
	defer n.mc.RUnlock()

	return ReadResponse{Type: "read_ok", Value: n.counter}, nil
}

func (n *CounterNode) Run() error {
	defer func() {
		close(n.shutdown)

		n.routines.Wait()
	}()

	return n.Node.Run()
}
