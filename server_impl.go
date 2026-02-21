package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"strconv"
	"time"
)

type clientMessage struct {
	addrUDP *lspnet.UDPAddr
	message *Message
}

type serverMessage struct {
	connId  int
	payload []byte
}

type unackedServerMsg struct {
	message         *Message
	numEpochsWaited int
	currBackoff     int
}

type clientInfo struct {
	clientAddrUDP    *lspnet.UDPAddr
	curSeqNumRead    int
	curSeqNumWrite   int
	receivedMessages map[int](*Message)
	unackedMessages  map[int](*unackedServerMsg)
	numDeadEpochs    int
	numUnackedMsgs   int
	windowStart      int
	windowEnd        int
	unwrittenQueue   *list.List
	sentMsgInEpoch   bool
	pendingClose     bool
}

type server struct {
	connUDP            *lspnet.UDPConn
	curConnId          int
	clients            map[int](*clientInfo)
	readRequest        chan bool
	readResponse       chan *Message
	messages           chan clientMessage
	timer              *time.Ticker
	params             *Params
	writeRequest       chan serverMessage
	readPending        bool
	allClients         map[*lspnet.UDPAddr](bool)
	close              chan bool
	pendingMsgsSent    chan bool
	closeReady         bool
	clientPendingClose chan int
	writeError         chan bool
	closeError         chan bool
}

func (s *server) NewClientInfo(initialSeqNum int, clientAddrUDP *lspnet.UDPAddr) *clientInfo {
	receivedMessages := make(map[int](*Message))
	unackedMessages := make(map[int](*unackedServerMsg))
	unwrittenQueue := list.New()
	newClientInfo := clientInfo{clientAddrUDP, initialSeqNum + 1, initialSeqNum + 1,
		receivedMessages, unackedMessages, 0, 0, initialSeqNum + 1,
		initialSeqNum + s.params.WindowSize, unwrittenQueue, false, false}
	return &newClientInfo
}

func NewServer(port int, params *Params) (Server, error) {
	addrUDP, err := lspnet.ResolveUDPAddr("udp", lspnet.JoinHostPort("", strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}
	connUDP, err := lspnet.ListenUDP("udp", addrUDP)
	if err != nil {
		return nil, err
	}
	readRequest := make(chan bool, 1)
	readResponse := make(chan *Message, 1)
	messages := make(chan clientMessage, 1)
	clients := make(map[int](*clientInfo))
	timer := time.NewTicker(time.Duration(params.EpochMillis) * time.Millisecond)
	writeRequest := make(chan serverMessage, 1)
	allClients := make(map[*lspnet.UDPAddr](bool))
	close := make(chan bool)
	pendingMsgsSent := make(chan bool)
	clientPendingClose := make(chan int)
	writeError := make(chan bool)
	closeError := make(chan bool)
	newServer := server{connUDP, 1, clients, readRequest, readResponse, messages,
		timer, params, writeRequest, false, allClients, close, pendingMsgsSent,
		false, clientPendingClose, writeError, closeError}

	go newServer.mainRoutine()
	go newServer.readRoutine()

	return &newServer, nil
}

func (s *server) Read() (int, []byte, error) {
	if s.closeReady {
		return 0, nil, errors.New("server closed")
	}
	s.readRequest <- true
	message := <-s.readResponse
	if message.SeqNum == 0 {
		return message.ConnID, nil, errors.New("read error")
	}
	return message.ConnID, message.Payload, nil
}

func (s *server) Write(connId int, payload []byte) error {
	if s.closeReady {
		return errors.New("server closed")
	}
	s.writeRequest <- serverMessage{connId, payload}
	if <-s.writeError {
		return errors.New("write error")
	}
	return nil
}

func (s *server) CloseConn(connId int) error {
	s.clientPendingClose <- connId
	if <-s.closeError {
		return errors.New("close conn error")
	}
	return nil
}

func (s *server) Close() error {
	s.close <- true
	<-s.pendingMsgsSent
	err := s.connUDP.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *server) mainRoutine() {
	for {
		select {
		case <-s.close:
			s.closeReady = true
			if s.checkCloseReady() {
				return
			}
		case connId := <-s.clientPendingClose:
			c, exists := s.clients[connId]
			if !exists {
				s.closeError <- true
				break
			} else {
				s.closeError <- false
			}
			s.allClients[c.clientAddrUDP] = false
			c.pendingClose = true
			if c.pendingClose && c.unwrittenQueue.Len() == 0 && len(c.unackedMessages) == 0 {
				delete(s.clients, connId)
			}
		case <-s.timer.C:
			for connId, c := range s.clients {
				curEpoch := c.numDeadEpochs
				if curEpoch > s.params.EpochLimit {
					s.allClients[c.clientAddrUDP] = false
					if s.readPending {
						s.handleReadPending(connId, c)
					}
				}
				for _, serverMsg := range c.unackedMessages {
					if serverMsg.numEpochsWaited >= serverMsg.currBackoff {
						err := s.writeMessage(c.clientAddrUDP, serverMsg.message)
						if err != nil {
							return
						}
						c.sentMsgInEpoch = true
						if serverMsg.currBackoff == 0 {
							serverMsg.currBackoff++
						} else {
							serverMsg.currBackoff = min(s.params.MaxBackOffInterval, serverMsg.currBackoff*2)
						}
						serverMsg.numEpochsWaited = 0
					} else {
						serverMsg.numEpochsWaited++
					}
				}
				if !c.sentMsgInEpoch {
					heartbeat := NewAck(connId, 0)
					err := s.writeMessage(c.clientAddrUDP, heartbeat)
					if err != nil {
						return
					}
				}
				c.sentMsgInEpoch = false
				c.numDeadEpochs++
			}
		case clientMessage := <-s.messages:
			message := clientMessage.message
			clientAddrUDP := clientMessage.addrUDP
			if message.Type == MsgConnect {
				_, exists := s.allClients[clientAddrUDP]
				if exists {
					break
				}
				s.allClients[clientAddrUDP] = true
				s.clients[s.curConnId] = s.NewClientInfo(message.SeqNum, clientAddrUDP)
				ackMsg := NewAck(s.curConnId, message.SeqNum)
				err := s.writeMessage(clientAddrUDP, ackMsg)
				if err != nil {
					return
				}
				s.curConnId++
			} else {
				clientID := message.ConnID
				clientInfo, exists := s.clients[clientID]
				if !exists {
					break
				}
				clientInfo.numDeadEpochs = 0
				if message.Type == MsgData {
					payload := message.Payload
					if len(payload) < message.Size {
						break
					} else if len(payload) > message.Size {
						message.Payload = payload[:message.Size]
					}
					checksum := CalculateChecksum(message.ConnID, message.SeqNum, message.Size, message.Payload)
					if checksum != message.Checksum {
						break
					}
					ackMsg := NewAck(message.ConnID, message.SeqNum)
					err := s.writeMessage(clientAddrUDP, ackMsg)
					if err != nil {
						return
					}
					if message.SeqNum < clientInfo.curSeqNumRead {
						break
					}
					clientInfo.receivedMessages[message.SeqNum] = message
					if s.readPending {
						s.handleReadPending(message.ConnID, clientInfo)
					}
				} else if message.Type == MsgAck {
					_, exists := clientInfo.unackedMessages[message.SeqNum]
					if !exists {
						break
					}
					clientInfo.numUnackedMsgs--
					delete(clientInfo.unackedMessages, message.SeqNum)
					if message.SeqNum == clientInfo.windowStart {
						mapLen := len(clientInfo.unackedMessages)
						if mapLen == 0 {
							clientInfo.windowStart += s.params.WindowSize
						} else {
							curSeqNum := clientInfo.windowStart + 1
							for curSeqNum <= clientInfo.windowEnd {
								_, exists = clientInfo.unackedMessages[curSeqNum]
								if exists {
									clientInfo.windowStart = curSeqNum
									break
								}
								curSeqNum++
							}
						}
						clientInfo.windowEnd = clientInfo.windowStart + s.params.WindowSize - 1
					}
					for clientInfo.unwrittenQueue.Len() != 0 {
						front := clientInfo.unwrittenQueue.Front()
						frontMsg, ok := front.Value.(*unackedServerMsg)
						if !ok {
							return
						}
						frontSeqNum := frontMsg.message.SeqNum
						if clientInfo.numUnackedMsgs < s.params.MaxUnackedMessages && frontSeqNum <= clientInfo.windowEnd {
							clientInfo.unackedMessages[frontSeqNum] = frontMsg
							err := s.writeMessage(clientInfo.clientAddrUDP, frontMsg.message)
							if err != nil {
								return
							}
							clientInfo.sentMsgInEpoch = true
							clientInfo.unwrittenQueue.Remove(front)
							clientInfo.numUnackedMsgs++
						} else {
							break
						}
					}
					if clientInfo.pendingClose && clientInfo.unwrittenQueue.Len() == 0 && len(clientInfo.unackedMessages) == 0 {
						delete(s.clients, clientID)
					}
					if s.checkCloseReady() {
						return
					}
				}
			}
		case <-s.readRequest:
			s.readPending = true
			for connId, clientInfo := range s.clients {
				if s.handleReadPending(connId, clientInfo) {
					break
				}
			}
		case serverMsg := <-s.writeRequest:
			connId := serverMsg.connId
			payload := serverMsg.payload
			clientInfo, exists := s.clients[connId]
			if !exists {
				s.writeError <- true
				break
			} else if !s.allClients[clientInfo.clientAddrUDP] {
				s.writeError <- true
				break
			} else {
				s.writeError <- false
			}
			checksum := CalculateChecksum(connId, clientInfo.curSeqNumWrite, len(payload), payload)
			dataMsg := NewData(connId, clientInfo.curSeqNumWrite, len(payload), payload, checksum)
			newUnackedMsg := &unackedServerMsg{dataMsg, 0, 0}
			if clientInfo.numUnackedMsgs < s.params.MaxUnackedMessages && clientInfo.curSeqNumWrite <= clientInfo.windowEnd {
				clientInfo.unackedMessages[clientInfo.curSeqNumWrite] = newUnackedMsg
				err := s.writeMessage(clientInfo.clientAddrUDP, dataMsg)
				if err != nil {
					return
				}
				clientInfo.sentMsgInEpoch = true
				clientInfo.numUnackedMsgs++
			} else {
				clientInfo.unwrittenQueue.PushBack(newUnackedMsg)
			}
			clientInfo.curSeqNumWrite++
			if clientInfo.pendingClose && clientInfo.unwrittenQueue.Len() == 0 && len(clientInfo.unackedMessages) == 0 {
				delete(s.clients, connId)
			}
			if s.checkCloseReady() {
				return
			}
		}
	}
}

func (s *server) readRoutine() {
	for {
		buffer := make([]byte, 2000)
		len, addr, err := s.connUDP.ReadFromUDP(buffer)
		if err != nil {
			return
		}
		var msg Message
		err = json.Unmarshal(buffer[:len], &msg)
		if err != nil {
			return
		}
		clientMsg := clientMessage{addr, &msg}
		s.messages <- clientMsg
	}
}

func (s *server) writeMessage(addrUDP *lspnet.UDPAddr, msg *Message) error {
	jsonString, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = s.connUDP.WriteToUDP(jsonString, addrUDP)
	if err != nil {
		return err
	}
	return nil
}

func (s *server) handleReadPending(connId int, c *clientInfo) bool {
	msg, exists := c.receivedMessages[c.curSeqNumRead]
	if exists {
		s.readResponse <- msg
		delete(c.receivedMessages, c.curSeqNumRead)
		c.curSeqNumRead++
		s.readPending = false
		return true
	} else if !s.allClients[c.clientAddrUDP] {
		s.readResponse <- NewAck(connId, 0)
		s.readPending = false
	}
	return false
}

func (s *server) checkCloseReady() bool {
	if s.closeReady {
		for _, c := range s.clients {
			if c.unwrittenQueue.Len() != 0 || len(c.unackedMessages) != 0 {
				return false
			}
		}
		s.pendingMsgsSent <- true
		s.timer.Stop()
		return true
	}
	return false
}
