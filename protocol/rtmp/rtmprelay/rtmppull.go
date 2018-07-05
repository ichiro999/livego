package rtmprelay

import (
	"net"
	"errors"
	"fmt"
	"github.com/livego/protocol/rtmp/core"
	"github.com/livego/av"
	log "github.com/livego/logging"
	"time"
	"sync"
)

type RtmpPull struct {
	url string
	clientConn *net.Conn
	isStart bool
	endChann chan int
	connectFlag bool
	connectPlayClient    *core.ConnClient
	writer Writer
	connLock sync.Locker
}

func NewRtmpPull(url string) *RtmpPull {
	return &RtmpPull{
		url: url,
		endChann: make(chan int),
		connectFlag: false,
		connectPlayClient: nil,
	}
}

func NewRtmpPullbyConn(url string, conn *net.Conn) *RtmpPull {
	return &RtmpPull{
		url: url,
		clientConn:conn,
		isStart: false,
		endChann: make(chan int),
		connectFlag: false,
		connectPlayClient: nil,
	}
}

func (self *RtmpPull) rtmpDisconnect() {
	self.connLock.Lock()
	defer func() {
		self.connLock.Unlock()
		if r := recover(); r != nil {
			log.Errorf("rtmppull rtmpDisconnect url(%s) panic:%v", self.url, r)
		}
	}()

	if self.connectPlayClient != nil && self.connectFlag {
		self.connectPlayClient.Close(nil)
		self.connectPlayClient = nil
		log.Warningf("rtmp pull disconnect, url=%s", self.url)
	}
	self.connectFlag = false
}

func (self *RtmpPull) rtmpConnect() error {
	self.connLock.Lock()
	defer func() {
		self.connLock.Unlock()
		if r := recover(); r != nil {
			log.Errorf("rtmppull rtmpDisconnect url(%s) panic:%v", self.url, r)
		}
	}()

	var err error

	self.connectPlayClient = core.NewConnClient()

	if self.clientConn != nil {
		err = self.connectPlayClient.StartOnConn(*self.clientConn, self.url, av.PLAY)
	} else {
		err = self.connectPlayClient.Start(self.url, av.PLAY)
	}

	if err != nil {
		log.Errorf("rtmp pull connect(%s) error:%v", self.url, err)
		self.connectPlayClient = nil
		self.connectFlag = false
		return err
	}
	log.Warningf("rtmp pull connect ok, url=%s", self.url)
	self.connectFlag = true
	return nil
}

func (self *RtmpPull) onWork() {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("rtmp pull onwork is over, url=%s, panic=%v", self.url, r)
		}
	}()
	for {
		if !self.isStart {
			break
		}

		if !self.connectFlag {
			self.rtmpConnect()
		}

		if !self.connectFlag {
			time.Sleep(2000*time.Millisecond)
			continue
		}
		csPacket := &core.ChunkStream{}
		err := self.connectPlayClient.Read(csPacket)
		if err != nil {
			self.rtmpDisconnect()
			continue
		}

		if csPacket.TypeID != av.TAG_AUDIO && csPacket.TypeID != av.TAG_VIDEO && csPacket.TypeID != av.TAG_SCRIPTDATAAMF0 {
			continue
		}
		if self.writer != nil {
			self.writer.Send(csPacket)
		}
	}
	self.rtmpDisconnect()
	self.endChann <- 1
}

func (self *RtmpPull) Start() error {
    if self.isStart {
        return errors.New(fmt.Sprintf("RtmpPull has already been started url=%s", self.url))
	}

	self.isStart = true

	log.Warningf("Rtmp pull start url=%s", self.url)
	go self.onWork()
	return nil
}

func (self *RtmpPull) Stop() {
	if !self.isStart {
		return
	}

	self.isStart = false
	log.Warningf("Rtmp pull stop url=%s", self.url)
	<- self.endChann
	return
}

func (self *RtmpPull) SetWriter(writer Writer) {
	self.writer = writer
}
