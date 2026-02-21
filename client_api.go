package lsp

type Client interface {
	ConnID() int
	Read() ([]byte, error)
	Write(payload []byte) error
	Close() error
}
