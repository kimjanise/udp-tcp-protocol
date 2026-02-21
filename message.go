package lsp

import (
	"fmt"
	"strconv"
)

type MsgType int

const (
	MsgConnect MsgType = iota
	MsgData
	MsgAck
	MsgCAck
)

type Message struct {
	Type     MsgType
	ConnID   int
	SeqNum   int
	Size     int
	Checksum uint16
	Payload  []byte
}

func NewConnect(initialSeqNum int) *Message {
	return &Message{
		Type:   MsgConnect,
		SeqNum: initialSeqNum,
	}
}

func NewData(connID, seqNum, size int, payload []byte, checksum uint16) *Message {
	return &Message{
		Type:     MsgData,
		ConnID:   connID,
		SeqNum:   seqNum,
		Size:     size,
		Payload:  payload,
		Checksum: checksum,
	}
}

func NewAck(connID, seqNum int) *Message {
	return &Message{
		Type:   MsgAck,
		ConnID: connID,
		SeqNum: seqNum,
	}
}

func NewCAck(connID, seqNum int) *Message {
	return &Message{
		Type:   MsgCAck,
		ConnID: connID,
		SeqNum: seqNum,
	}
}

func (m *Message) String() string {
	var name, payload, checksum string
	switch m.Type {
	case MsgConnect:
		name = "Connect"
	case MsgData:
		name = "Data"
		checksum = " " + strconv.Itoa(int(m.Checksum))
		payload = " " + string(m.Payload)
	case MsgAck:
		name = "Ack"
	case MsgCAck:
		name = "CAck"
	}
	return fmt.Sprintf("[%s %d %d%s%s]", name, m.ConnID, m.SeqNum, checksum, payload)
}
