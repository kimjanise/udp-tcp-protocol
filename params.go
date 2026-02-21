package lsp

import "fmt"

const (
	DefaultEpochLimit         = 5
	DefaultEpochMillis        = 2000
	DefaultWindowSize         = 1
	DefaultMaxBackOffInterval = 0
	DefaultMaxUnackedMessages = 1
)

type Params struct {
	EpochLimit         int
	EpochMillis        int
	WindowSize         int
	MaxBackOffInterval int
	MaxUnackedMessages int
}

func NewParams() *Params {
	return &Params{
		EpochLimit:         DefaultEpochLimit,
		EpochMillis:        DefaultEpochMillis,
		WindowSize:         DefaultWindowSize,
		MaxBackOffInterval: DefaultMaxBackOffInterval,
		MaxUnackedMessages: DefaultMaxUnackedMessages,
	}
}

func (p *Params) String() string {
	return fmt.Sprintf("[EpochLimit: %d, EpochMillis: %d, WindowSize: %d, MaxBackOffInterval: %d,"+
		"MaxUnackedMessages: %d]",
		p.EpochLimit, p.EpochMillis, p.WindowSize, p.MaxBackOffInterval, p.MaxUnackedMessages)
}
