package rtmprelay

import (
	"github.com/livego/protocol/rtmp/core"
)

type Writer interface {
	Start() error
	Stop()
	Send(csPacket *core.ChunkStream) error
	GetInfo() string
}
