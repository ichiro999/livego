package rtmprelay

import (
	"errors"
	"fmt"
	"net"
	"github.com/livego/av"
	"github.com/livego/protocol/rtmp/core"
	"github.com/emirpasic/gods/lists/singlylinkedlist"
	log "github.com/livego/logging"
	"sync"
	"time"
)

var RELAY_BUFFER_MAX = 2000 //ms

type RtmpPush struct {
	url string
	clientConn *net.Conn
	isStart bool
	endChann chan int
	connectFlag bool
	connectPushClient *core.ConnClient
	mediaPacketList *singlylinkedlist.List
	queueLock sync.RWMutex
	signalChan chan int
	videoHdr   []byte
	audioHdr   []byte
	isVideoHdrSent bool
	isAudioHdrSent bool
}

func NewRtmpPush(url string) *RtmpPush {
	return &RtmpPush{
		url:url,
		clientConn:nil,
		isStart: false,
		endChann: make(chan int),
		connectFlag: false,
		connectPushClient: nil,
		mediaPacketList: singlylinkedlist.New(),
		signalChan: make(chan int, RELAY_BUFFER_MAX),
	}
}

func NewRtmpPushByConn(url string, conn *net.Conn) *RtmpPush {
	return &RtmpPush{
		url:url,
		clientConn:conn,
		isStart: false,
		endChann: make(chan int),
		connectFlag: false,
		connectPushClient: nil,
	}
}

func (self *RtmpPush) rtmpDisconnect() {
	if self.connectPushClient != nil && self.connectFlag {
		self.connectPushClient.Close(nil)
		self.connectPushClient = nil
		log.Warningf("rtmp push disconnect, url=%s", self.url)
	}
	self.isAudioHdrSent = false
	self.isVideoHdrSent = false
	self.connectFlag = false
}

func (self *RtmpPush) rtmpConnect() error {
	var err error
	self.connectPushClient = core.NewConnClient()

	if self.clientConn != nil {
		err = self.connectPushClient.StartOnConn(*self.clientConn, self.url, av.PUBLISH)
	} else {
		err = self.connectPushClient.Start(self.url, av.PUBLISH)
	}

	if err != nil {
		log.Errorf("rtmp push connect(%s) error:%v", self.url, err)
		self.connectPushClient = nil
		return err
	}
	log.Warningf("rtmp push connect ok, url=%s", self.url)
	self.connectFlag = true
	return nil
}

func (self *RtmpPush) sendVideoHdr(timestamp uint32) error {
    if len(self.videoHdr) == 0 {
    	return nil
	}

	csPacket := core.ChunkStream{}
	csPacket.Data = self.videoHdr
	csPacket.Length = uint32(len(self.videoHdr))
	csPacket.Timestamp = timestamp
	csPacket.StreamID = self.connectPushClient.GetStreamId()
	csPacket.TypeID = av.TAG_VIDEO

	err := self.connectPushClient.Write(csPacket)
	if err != nil {
		log.Errorf("rtmp send video hdr error:%v, url=%s", err, self.url)
		return err
	}

	log.InfoBody(self.videoHdr[:], fmt.Sprintf("rtmp send video pps/sps ok, url=%s", self.url))
	self.isVideoHdrSent = true
	return nil
}

func (self *RtmpPush) sendAudioHdr(timestamp uint32) error {
	if len(self.audioHdr) == 0 {
		return nil
	}

	csPacket := core.ChunkStream{}
	csPacket.Data = self.audioHdr
	csPacket.Length = uint32(len(self.audioHdr))
	csPacket.Timestamp = timestamp
	csPacket.StreamID = self.connectPushClient.GetStreamId()
	csPacket.TypeID = av.TAG_AUDIO

	err := self.connectPushClient.Write(csPacket)
	if err != nil {
		log.Errorf("rtmp send audio hdr error:%v, url=%s", err, self.url)
		return err
	}

	log.InfoBody(self.audioHdr[:], fmt.Sprintf("rtmp audio asc ok, url=%s", self.url))
	self.isAudioHdrSent = true
	return nil
}

func (self *RtmpPush) sendOnMeta(timestamp uint32) error {
	err := self.connectPushClient.WriteBaseMeta("1.0", 15, 0, 0, 44100, 16)
	if err != nil {
		log.Errorf("rtmp push onMeta error:%v", err)
		return err
	}
	log.Warningf("rtmp push onMeta ok, url=%s", self.url)
	return nil
}

func (self *RtmpPush) onWork() {
	defer func() {
		log.Warningf("rtmp push onWork is over, url=%s", self.url)
		if r := recover(); r != nil {
			log.Errorf("rtmppush onwork is over, url(%s), panic:%v", self.url, r)
		}
	}()
	log.Warningf("rtmp push onWork is running, url=%s", self.url)
	for {
		if !self.isStart {
			break
		}

		ok := self.wait()
		if !ok {
			break
		}

		csPacket := self.getDataFromQueue()
		if csPacket == nil {
			continue
		}

		if av.IsVideoHdr(csPacket.Data) {
			if len(self.videoHdr) != 0 {
				self.videoHdr =self.videoHdr[:0]
			}
			self.videoHdr = append(self.videoHdr, csPacket.Data[:]...)
			self.isVideoHdrSent = false
		} else if av.IsAudioHdr(csPacket.Data) {
			if len(self.audioHdr) != 0 {
				self.audioHdr = self.audioHdr[:0]
			}
			self.audioHdr = append(self.audioHdr, csPacket.Data[:]...)
			self.isAudioHdrSent = false
		}
		if !self.connectFlag {
			err := self.rtmpConnect()
			if err == nil {
				self.sendOnMeta(csPacket.Timestamp)
				self.sendVideoHdr(csPacket.Timestamp)
				self.sendAudioHdr(csPacket.Timestamp)
			}
		}

		if !self.connectFlag {
			time.Sleep(2000*time.Millisecond)
			continue
		}

		if av.IsVideoHdr(csPacket.Data) && self.isVideoHdrSent {
			continue;
		} else if av.IsAudioHdr(csPacket.Data) && self.isAudioHdrSent {
			continue;
		}

		csPacket.StreamID = self.connectPushClient.GetStreamId()
		err := self.connectPushClient.Write(*csPacket)
		if err != nil {
			log.Errorf("rtmppush write(%s) error:%v", self.url, err)
			self.rtmpDisconnect()
			time.Sleep(250*time.Millisecond)
		}
	}
	log.Warningf("rtmppush is over, url=%s", self.url)
	self.rtmpDisconnect()
	self.endChann <- 1
}

func (self *RtmpPush) Start() error {
	if self.isStart {
        return errors.New(fmt.Sprintf("Rtmp push has been already started, url=%s", self.url))
	}

	self.isStart = true
	log.Warningf("rtmp push start url=%s", self.url)
	go self.onWork()
	return nil
}

func (self *RtmpPush) Stop() {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("rtmppush stop url(%s) panic:%v", self.url, r)
		}
	}()
	if !self.isStart {
		return
	}

	self.isStart = false
	close(self.signalChan)
	log.Warningf("Rtmp push stop, url=%s", self.url)
	<- self.endChann

	close(self.endChann)
}

func (self *RtmpPush) notify() {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("rtmp push(url=%s) notify panic:%v", r)
		}
	}()
	self.signalChan <- 1
}

func (self *RtmpPush) wait() (ret bool) {
	select {
	case _, ret = <- self.signalChan:
		break
	case <- time.After(200*time.Millisecond):
		ret = true
		break
	}

	return
}

func (self *RtmpPush) insertQueue(cs *core.ChunkStream) int {
    self.queueLock.Lock()
    defer self.queueLock.Unlock()

    if self.mediaPacketList.Size() > RELAY_BUFFER_MAX {
    	self.mediaPacketList.Clear()
	}
    self.mediaPacketList.Add(cs)

    return self.mediaPacketList.Size()
}

func (self *RtmpPush) getDataFromQueue() *core.ChunkStream {
	self.queueLock.Lock()
	defer self.queueLock.Unlock()

	if self.mediaPacketList.Size() == 0 {
		return nil
	}

	value, _ := self.mediaPacketList.Get(0)
	cs := value.(*core.ChunkStream)

	self.mediaPacketList.Remove(0)
	return cs
}

func (self *RtmpPush) Send(cs *core.ChunkStream) error {
	if !self.isStart {
		return nil
	}

	self.insertQueue(cs)
	self.notify()
	return nil
}

func (self *RtmpPush) GetInfo() string {
	return self.url
}