package coordinator

import (
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"os"
	"strconv"
	"sync"
	"time"
	"tp1/globalconfig"
	"tp1/protobuf/protopb"
	"tp1/rabbitmq"
)

var (
	nodeId       = os.Getenv("NODE_NAME")
	expectedACKs = os.Getenv("FILTER_NODES")
)

type EOFLeader struct {
	mu            sync.Mutex
	ackReceived   map[string]map[string]bool
	leadingFor    map[string]bool
	notLeadingFor map[string]bool
	queue         string
	log           *logging.Logger
	channel       *amqp.Channel
	groupKey      string
}

func NewEOFLeader(log *logging.Logger, channel *amqp.Channel, groupKey string) *EOFLeader {
	err := rabbitmq.DeclareTopicExchanges(channel, globalconfig.CoordinationExchange)
	if err != nil {
		log.Fatalf("failed to declare coordination exchange: %v", err)
	}

	queue, err := rabbitmq.DeclareTemporaryQueue(channel)
	if err != nil {
		log.Fatalf("failed to declare temporary queue: %v", err)
	}

	err = rabbitmq.BindQueueToExchange(channel, queue.Name, globalconfig.CoordinationExchange, groupKey)
	if err != nil {
		log.Fatalf("failed to bind queue to exchange: %v", err)
	}

	leader := &EOFLeader{
		ackReceived:   make(map[string]map[string]bool),
		leadingFor:    make(map[string]bool),
		notLeadingFor: make(map[string]bool),
		queue:         queue.Name,
		log:           log,
		channel:       channel,
		groupKey:      groupKey,
	}

	go leader.startListening()

	return leader
}

func (e *EOFLeader) TakeLeadership(clientID string) {
	msg := protopb.CoordinationMessage{
		ClientId: proto.String(clientID),
		NodeId:   proto.String(nodeId),
		Type:     protopb.MessageType_LEADER.Enum(),
	}

	data, err := proto.Marshal(&msg)
	if err != nil {
		e.log.Fatalf("[client_id:%s][leader] failed to marshal leader message: %v", clientID, err)
	}

	err = rabbitmq.Publish(e.channel, globalconfig.CoordinationExchange, e.groupKey, data)
	if err != nil {
		e.log.Fatalf("[client_id:%s][leader] failed to publish leader message: %v", clientID, err)
	}

	e.leadingFor[clientID] = true
	e.log.Infof("[client_id:%s][leader] broadcast leader message", clientID)
}

func (e *EOFLeader) WaitForACKs(clientID string) {
	ackTimeout := time.After(5 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ackTimeout:
			e.log.Warningf("[client_id:%s][leader] timeout waiting for acks", clientID)
			// TODO: clear ACK map and leadingFor map
			return
		case <-ticker.C:
			e.mu.Lock()
			receivedACKs := len(e.ackReceived[clientID])
			e.mu.Unlock()

			if receivedACKs >= e.getExpectedACKs() {
				e.log.Infof("[client_id:%s][leader] received all acks: %d/%d", clientID, receivedACKs, e.getExpectedACKs())
				// TODO: clear ACK map and leadingFor map
				return
			}
		}
	}

}

func (e *EOFLeader) SendACKs(_ string) {
	// TODO: remove parameter
	for c := range e.notLeadingFor {
		/*if c != clientID {
			e.sendACK(c)
		}*/
		e.sendACK(c)
		// TODO: delete after the loop delete(e.notLeadingFor, clientID)
	}
}

func (e *EOFLeader) startListening() {
	msgs, err := rabbitmq.ConsumeFromQueue(e.channel, e.queue)
	if err != nil {
		e.log.Fatalf("failed to start consuming: %v", err)
	}

	for msg := range msgs {
		var coordMsg protopb.CoordinationMessage
		err := proto.Unmarshal(msg.Body, &coordMsg)
		if err != nil {
			e.log.Errorf("failed to unmarshal coordination message: %v", err)
			continue
		}

		clientID := coordMsg.GetClientId()
		msgNodeId := coordMsg.GetNodeId()

		if coordMsg.GetType() == protopb.MessageType_ACK {
			_, found := e.leadingFor[clientID]
			if found {
				e.log.Infof("[client_id:%s][node] received ack from %s", clientID, msgNodeId)
				e.collectAck(clientID, msgNodeId)
			}
		} else if coordMsg.GetType() == protopb.MessageType_LEADER {
			if msgNodeId != nodeId {
				e.log.Infof("[client_id:%s][node] received leader message from %s", clientID, msgNodeId)
				e.notLeadingFor[clientID] = true
			}
		}
	}
}

func (e *EOFLeader) collectAck(clientId string, nodeId string) {
	_, found := e.ackReceived[clientId]
	if !found {
		e.ackReceived[clientId] = make(map[string]bool)
	}
	e.ackReceived[clientId][nodeId] = true
}

func (e *EOFLeader) getExpectedACKs() int {
	acks, err := strconv.Atoi(expectedACKs)
	if err != nil {
		e.log.Fatalf("failed to parse expected ACKs: %v", err)
	}

	return acks - 1
}

func (e *EOFLeader) sendACK(clientID string) {
	msg := protopb.CoordinationMessage{
		ClientId: proto.String(clientID),
		NodeId:   proto.String(nodeId),
		Type:     protopb.MessageType_ACK.Enum(),
	}

	data, err := proto.Marshal(&msg)
	if err != nil {
		e.log.Fatalf("[client_id:%s][node] failed to marshal ack message: %v", clientID, err)
	}

	err = rabbitmq.Publish(e.channel, globalconfig.CoordinationExchange, e.groupKey, data)
	if err != nil {
		e.log.Fatalf("[client_id:%s][node] failed to publish ack message: %v", clientID, err)
	}

	e.log.Infof("[client_id:%s][node] sent ack message", clientID)
}
