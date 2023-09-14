package log

import (
	"encoding/json"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const logKey = "log"

type LogNode struct {
	maelstrom.Node
	neighbours []string
	m          sync.Mutex
	log        map[string][][2]int
	committed  map[string]int
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

func NewLogNode() *LogNode {
	n := &LogNode{
		Node:      *maelstrom.NewNode(),
		log:       make(map[string][][2]int),
		committed: make(map[string]int),
	}

	n.Handle("topology", MaelstromHandler(&n.Node, n.Topology))
	n.Handle("send", MaelstromHandler(&n.Node, n.Send))
	n.Handle("poll", MaelstromHandler(&n.Node, n.Poll))
	n.Handle("commit_offsets", MaelstromHandler(&n.Node, n.CommitOffsets))
	n.Handle("list_committed_offsets", MaelstromHandler(&n.Node, n.ListCommittedOffsets))

	return n
}

type TopologyRequest struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type TopologyResponse struct {
	Type string `json:"type"`
}

func (n *LogNode) Topology(request Message[TopologyRequest]) (
	TopologyResponse,
	error,
) {
	n.m.Lock()
	defer n.m.Unlock()
	for _, nb := range request.Body.Topology[n.ID()] {
		n.neighbours = append(n.neighbours, nb)
	}

	return TopologyResponse{Type: "topology_ok"}, nil
}

type SendRequest struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	Msg  int    `json:"msg"`
}

type SendResponse struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

func (n *LogNode) Send(request Message[SendRequest]) (
	SendResponse,
	error,
) {
	n.m.Lock()
	defer n.m.Unlock()
	offset := len(n.log[request.Body.Key])
	n.log[request.Body.Key] = append(n.log[request.Body.Key], [2]int{
		offset,
		request.Body.Msg,
	})

	return SendResponse{
		Type:   "send_ok",
		Offset: offset,
	}, nil

}

type PollRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type PollResponse struct {
	Type string              `json:"type"`
	Msgs map[string][][2]int `json:"msgs"`
}

func (n *LogNode) Poll(request Message[PollRequest]) (
	PollResponse,
	error,
) {
	n.m.Lock()
	defer n.m.Unlock()
	res := PollResponse{
		Type: "poll_ok",
		Msgs: make(map[string][][2]int),
	}

	for key, offset := range request.Body.Offsets {
		res.Msgs[key] = n.log[key][offset:]
	}

	return res, nil
}

type CommitOffsetsRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type CommitOffsetsResponse struct {
	Type string `json:"type"`
}

func (n *LogNode) CommitOffsets(request Message[CommitOffsetsRequest]) (
	CommitOffsetsResponse,
	error,
) {
	n.m.Lock()
	defer n.m.Unlock()
	for key, offset := range request.Body.Offsets {
		n.committed[key] = offset
	}

	return CommitOffsetsResponse{
		Type: "commit_offsets_ok",
	}, nil
}

type ListCommittedOffsetsRequest struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type ListCommittedOffsetsResponse struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

func (n *LogNode) ListCommittedOffsets(request Message[ListCommittedOffsetsRequest]) (
	ListCommittedOffsetsResponse,
	error,
) {
	n.m.Lock()
	defer n.m.Unlock()

	res := ListCommittedOffsetsResponse{
		Type:    "list_committed_offsets_ok",
		Offsets: make(map[string]int),
	}

	for _, key := range request.Body.Keys {
		res.Offsets[key] = n.committed[key]
	}

	return res, nil
}
