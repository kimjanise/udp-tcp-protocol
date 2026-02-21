package lsp

type Server interface {
	Read() (int, []byte, error)
	Write(connID int, payload []byte) error
	CloseConn(connID int) error
	Close() error
}
