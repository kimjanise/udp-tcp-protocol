package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"time"

	"github.com/cmu440/lspnet"
)

type unackedClientMsg struct {
	message         *Message
	numEpochsWaited int
	currBackoff     int
}
type client struct {
	connUDP          *lspnet.UDPConn
	connID           int
	curSeqNumWrite   int
	curSeqNumRead    int
	readRequest      chan bool
	readPending      bool
	readReady        chan []byte
	messages         chan *Message
	receivedMessages map[int]([]byte)
	timer            *time.Ticker
	params           *Params
	numDeadEpochs    int
	unackedMessages  map[int](*unackedClientMsg)
	writeRequest     chan []byte
	unwrittenQueue   *list.List
	numUnackedMsgs   int
	windowStart      int
	windowEnd        int
	sentMsgInEpoch   bool
	close            chan bool
	pendingMsgsSent  chan bool
	closeReady       bool
	connectionLost   bool
}

func NewClient(hostport string, initialSeqNum int, params *Params) (Client, error) {
	addrUDP, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	connUDP, err := lspnet.DialUDP("udp", nil, addrUDP)
	if err != nil {
		return nil, err
	}

	timer := time.NewTicker(time.Duration(params.EpochMillis) * time.Millisecond)
	connectMsg := NewConnect(initialSeqNum)
	err = writeMessage(connUDP, connectMsg)
	if err != nil {
		return nil, err
	}

	checkAck := make(chan int)
	go func() {
		msg, err := readMessage(connUDP)
		if err != nil {
			return
		}
		if msg.Type == MsgAck && msg.SeqNum == initialSeqNum {
			checkAck <- msg.ConnID
			return
		}
	}()

	connID := 0
	currEpoch := 0
	for connID == 0 {
		select {
		case <-timer.C:
			if currEpoch > params.EpochLimit {
				return nil, nil
			}
			err = writeMessage(connUDP, connectMsg)
			if err != nil {
				return nil, err
			}
			currEpoch++
		case id := <-checkAck:
			connID = id
		}
	}

	readRequest := make(chan bool, 1)
	readReady := make(chan []byte, 1)
	messages := make(chan *Message, 1)
	receivedMessages := make(map[int]([]byte))
	unackedMessages := make(map[int](*unackedClientMsg))
	writeRequest := make(chan []byte, 1)
	unwrittenQueue := list.New()
	close := make(chan bool)
	pendingMsgsSent := make(chan bool)
	newClient := client{connUDP, connID, initialSeqNum + 1, initialSeqNum + 1,
		readRequest, false, readReady, messages, receivedMessages,
		timer, params, 0, unackedMessages, writeRequest, unwrittenQueue,
		0, initialSeqNum + 1, initialSeqNum + params.WindowSize,
		false, close, pendingMsgsSent, false, false}

	go newClient.mainRoutine()
	go newClient.readRoutine()

	return &newClient, nil
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	if c.closeReady {
		return nil, errors.New("client closed")
	}
	c.readRequest <- true
	res := <-c.readReady
	if res == nil {
		return nil, errors.New("read error")
	}
	return res, nil
}

func (c *client) Write(payload []byte) error {
	if c.closeReady || c.connectionLost {
		return errors.New("write error")
	}
	c.writeRequest <- payload
	return nil
}

func (c *client) Close() error {
	c.close <- true
	<-c.pendingMsgsSent
	err := c.connUDP.Close()
	if err != nil {
		return err
	}
	return nil
}

func (c *client) mainRoutine() {
	for {
		select {
		case <-c.close:
			c.closeReady = true
			if c.checkCloseReady() {
				return
			}
		case <-c.timer.C:
			if c.numDeadEpochs > c.params.EpochLimit {
				c.connectionLost = true
				if c.readPending {
					c.handleReadPending()
				}
			}

			for _, clientMsg := range c.unackedMessages {
				if clientMsg.numEpochsWaited >= clientMsg.currBackoff {
					err := writeMessage(c.connUDP, clientMsg.message)
					if err != nil {
						return
					}
					c.sentMsgInEpoch = true
					if clientMsg.currBackoff == 0 {
						clientMsg.currBackoff++
					} else {
						clientMsg.currBackoff = min(c.params.MaxBackOffInterval, clientMsg.currBackoff*2)
					}
					clientMsg.numEpochsWaited = 0
				} else {
					clientMsg.numEpochsWaited++
				}
			}
			if !c.sentMsgInEpoch {
				heartbeat := NewAck(c.connID, 0)
				err := writeMessage(c.connUDP, heartbeat)
				if err != nil {
					return
				}
			}
			c.sentMsgInEpoch = false
			c.numDeadEpochs++
		case msg := <-c.messages:
			c.numDeadEpochs = 0
			if msg.Type == MsgData {
				payload := msg.Payload
				if len(payload) < msg.Size {
					break
				} else if len(payload) > msg.Size {
					payload = payload[:msg.Size]
				}
				checksum := CalculateChecksum(msg.ConnID, msg.SeqNum, msg.Size, payload)
				if checksum != msg.Checksum {
					break
				}
				ack := NewAck(msg.ConnID, msg.SeqNum)
				err := writeMessage(c.connUDP, ack)
				if err != nil {
					return
				}
				if msg.SeqNum < c.curSeqNumRead {
					break
				}
				c.receivedMessages[msg.SeqNum] = payload
				if c.readPending {
					c.handleReadPending()
				}
			}
			if msg.Type == MsgAck || msg.Type == MsgCAck {
				shiftWindow := false
				if msg.Type == MsgAck {
					_, exists := c.unackedMessages[msg.SeqNum]
					if !exists {
						break
					}
					delete(c.unackedMessages, msg.SeqNum)
					c.numUnackedMsgs--
					shiftWindow = msg.SeqNum == c.windowStart
				} else if msg.Type == MsgCAck {
					if msg.SeqNum < c.windowStart {
						break
					}
					curSeqNum := c.windowStart
					end := min(c.windowEnd, msg.SeqNum)
					for curSeqNum <= end {
						_, exists := c.unackedMessages[curSeqNum]
						if exists {
							delete(c.unackedMessages, curSeqNum)
							c.numUnackedMsgs--
						}
					}
					shiftWindow = msg.SeqNum >= c.windowStart
				}
				if shiftWindow {
					mapLen := len(c.unackedMessages)
					if mapLen == 0 {
						c.windowStart += c.params.WindowSize
					} else {
						curSeqNum := c.windowStart + 1
						for curSeqNum <= c.windowEnd {
							_, exists := c.unackedMessages[curSeqNum]
							if exists {
								c.windowStart = curSeqNum
								break
							}
							curSeqNum++
						}
					}
					c.windowEnd = c.windowStart + c.params.WindowSize - 1
				}
				for c.unwrittenQueue.Len() != 0 {
					front := c.unwrittenQueue.Front()
					frontMsg, ok := front.Value.(*unackedClientMsg)
					if !ok {
						return
					}
					frontSeqNum := frontMsg.message.SeqNum
					if c.numUnackedMsgs < c.params.MaxUnackedMessages && frontSeqNum <= c.windowEnd {
						c.unackedMessages[frontSeqNum] = frontMsg
						err := writeMessage(c.connUDP, frontMsg.message)
						if err != nil {
							return
						}
						c.sentMsgInEpoch = true
						c.unwrittenQueue.Remove(front)
						c.numUnackedMsgs++
					} else {
						break
					}
				}
				if c.checkCloseReady() {
					return
				}
			}
		case payload := <-c.writeRequest:
			checksum := CalculateChecksum(c.connID, c.curSeqNumWrite, len(payload), payload)
			dataMsg := NewData(c.connID, c.curSeqNumWrite, len(payload), payload, checksum)
			newUnackedMessage := &unackedClientMsg{dataMsg, 0, 0}
			if (c.numUnackedMsgs < c.params.MaxUnackedMessages) && (c.curSeqNumWrite <= c.windowEnd) {
				c.unackedMessages[c.curSeqNumWrite] = newUnackedMessage
				err := writeMessage(c.connUDP, dataMsg)
				if err != nil {
					return
				}
				c.numUnackedMsgs++
				c.sentMsgInEpoch = true
			} else {
				c.unwrittenQueue.PushBack(newUnackedMessage)
			}
			c.curSeqNumWrite++
			if c.checkCloseReady() {
				return
			}
		case <-c.readRequest:
			c.readPending = true
			c.handleReadPending()
		}
	}
}

func (c *client) readRoutine() {
	for {
		msg, err := readMessage(c.connUDP)
		if err != nil {
			return
		}
		c.messages <- msg
	}
}

func writeMessage(connUDP *lspnet.UDPConn, msg *Message) error {
	jsonString, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = connUDP.Write(jsonString)
	if err != nil {
		return err
	}
	return nil
}

func readMessage(connUDP *lspnet.UDPConn) (*Message, error) {
	buffer := make([]byte, 2000)
	len, err := connUDP.Read(buffer)
	if err != nil {
		return nil, err
	}
	var msg Message
	err = json.Unmarshal(buffer[:len], &msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (c *client) checkCloseReady() bool {
	if c.closeReady && c.unwrittenQueue.Len() == 0 && len(c.unackedMessages) == 0 {
		c.pendingMsgsSent <- true
		c.timer.Stop()
		return true
	}
	return false
}

func (c *client) handleReadPending() {
	payload, exists := c.receivedMessages[c.curSeqNumRead]
	if exists {
		c.readReady <- payload
		delete(c.receivedMessages, c.curSeqNumRead)
		c.curSeqNumRead++
		c.readPending = false
	} else if c.connectionLost {
		c.readReady <- nil
		c.readPending = false
	}
}
