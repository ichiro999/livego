package rtmp

import (
	"errors"
	"github.com/livego/av"
	"github.com/livego/concurrent-map"
	"github.com/livego/configure"
	log "github.com/livego/logging"
	"github.com/livego/protocol/rtmp/cache"
	"github.com/livego/protocol/rtmp/rtmprelay"
	"reflect"
	"fmt"
	"os/exec"
	"time"
	"github.com/livego/protocol/rtmp/core"
)

var (
	EmptyID = ""
)

type RtmpStream struct {
	streams cmap.ConcurrentMap //key
	upstreamMgrObj *UpstreamMgr
}

func NewRtmpStream() *RtmpStream {
	ret := &RtmpStream{
		streams: cmap.New(),

	}
	ret.InitUpstreamMgr()
	go ret.CheckAlive()
	return ret
}

func (rs *RtmpStream) InitUpstreamMgr(){
    rs.upstreamMgrObj = NewUpstreaMgr(rs)
	rs.upstreamMgrObj.Start()
}

func (rs *RtmpStream) IsExist(r av.ReadCloser) bool {
	info := r.Info()

	i, ok := rs.streams.Get(info.Key)
	if _, ok = i.(*Stream); ok {
		log.Errorf("Exist already info[%v]", info)
		return true
	}

	return false
}

func (rs *RtmpStream) HandleReader(r av.ReadCloser) (err error) {
	err = nil
	info := r.Info()
	log.Infof("HandleReader: info[%v]", info)

	var stream *Stream
	i, ok := rs.streams.Get(info.Key)
	if stream, ok = i.(*Stream); ok {
		if stream.IsPublishFlag {
			log.Warningf("stream publish is not over, info[%s:%s]", stream.GetInfo().Key, stream.GetInfo().UID)
			err = errors.New(fmt.Sprintf("stream publish is not over, info[%s:%s]", stream.GetInfo().Key, stream.GetInfo().UID))
			return
		}
		stream.TransStop()
		id := stream.ID()
		if id != EmptyID && id != info.UID {
			ns := NewStream(true)
			stream.Copy(ns)
			stream = ns
			stream.info = info
			rs.streams.Set(info.Key, ns)
		}
	} else {
		stream = NewStream(true)
		rs.streams.Set(info.Key, stream)
		stream.info = info
	}

	stream.AddReader(r)
	return
}

func (rs *RtmpStream) HandleWriter(w av.WriteCloser) {
	info := w.Info()
	log.Infof("HandleWriter: info[%v], type=%v", info, reflect.TypeOf(w))

	var s *Stream
	ok := rs.streams.Has(info.Key)
	if !ok {
		log.Warningf("No rtmp stream exist, create new Stream, info:%v", info)
		rs.upstreamMgrObj.Notify(info.Key)

		s = NewStream(false)
		rs.streams.Set(info.Key, s)
		s.AddWriter(w)
		s.info = info
	} else {
		item, ok := rs.streams.Get(info.Key)
		if ok {
			s = item.(*Stream)
			s.AddWriter(w)
		}
	}
}

func (rs *RtmpStream) GetStreams() cmap.ConcurrentMap {
	return rs.streams
}

func (rs *RtmpStream) CheckAlive() {
	for {
		<-time.After(300 * time.Millisecond)
		//log.Infof("RtmpStream.CheckAlive count=%d", rs.streams.Count())
		for item := range rs.streams.IterBuffered() {
			v := item.Val.(*Stream)
			if v.CheckAlive() == 0 {
				log.Infof("RtmpStream.CheckAlive remove info:%s", v.info)
				rs.streams.Remove(item.Key)
			}
		}
	}
}

type Stream struct {
	isStart       bool
	cache         *cache.Cache
	r             av.ReadCloser
	ws            cmap.ConcurrentMap
	info          av.Info
	IsPublishFlag bool
}

type PackWriterCloser struct {
	init bool
	w    av.WriteCloser
}

func (p *PackWriterCloser) GetWriter() av.WriteCloser {
	return p.w
}

func NewStream(isPublishFlag bool) *Stream {
	return &Stream{
		cache:        cache.NewCache(),
		ws:           cmap.New(),
		IsPublishFlag:isPublishFlag,
	}
}

func (s *Stream) ID() string {
	if s.r != nil {
		return s.r.Info().UID
	}
	return EmptyID
}

func (s *Stream) GetReader() av.ReadCloser {
	return s.r
}

func (s *Stream) GetInfo() av.Info {
	return s.info
}

func (s *Stream) GetWs() cmap.ConcurrentMap {
	return s.ws
}

func (s *Stream) Copy(dst *Stream) {
	for item := range s.ws.IterBuffered() {
		v := item.Val.(*PackWriterCloser)
		s.ws.Remove(item.Key)
		v.w.CalcBaseTimestamp()
		dst.AddWriter(v.w)
	}
}

func (s *Stream) AddReader(r av.ReadCloser) {
	s.r = r
	log.Infof("AddReader:%v", s.info)
	go s.TransStart()
}

func (s *Stream) AddWriter(w av.WriteCloser) {
	info := w.Info()
	pw := &PackWriterCloser{w: w}
	s.ws.Set(info.UID, pw)

}

/*检测本application下是否配置static_push,
如果配置, 启动push远端的连接*/
func (s *Stream) StartStaticPush() (ret bool) {
	ret = false
	log.Infof("StartStaticPush: current url=%s", s.info.URL)
	connTypelist, pushurllist, err := rtmprelay.GetStaticPushList(s.info.URL)
	if err != nil || len(pushurllist) < 1 {
		log.Errorf("StartStaticPush: GetStaticPushList error=%v", err)
		return
	}

	for index, pushurl := range pushurllist {
		if index >= len(connTypelist) {
			log.Errorf("StartStaticPush connType len=%d, index=%d", len(connTypelist), index)
			break
		}
		log.Infof("StartStaticPush: static pushurl=%s, conntype=%s", pushurl, connTypelist[index])

		staticpushObj := rtmprelay.GetAndCreateStaticPushObject(connTypelist[index], pushurl)
		if staticpushObj != nil {
			if err := staticpushObj.Start(); err != nil {
				log.Errorf("StartStaticPush: staticpushObj.Start %s error=%v", pushurl, err)
			} else {
				log.Infof("StartStaticPush: staticpushObj.Start %s ok", pushurl)
				ret = true
			}
		} else {
			log.Errorf("StartStaticPush GetStaticPushObject %s error", pushurl)
		}
	}

	return
}

func (s *Stream) StopStaticPush() {
	log.Infof("StopStaticPush: current url=%s", s.info.URL)
	_, pushurllist, err := rtmprelay.GetStaticPushList(s.info.URL)
	if err != nil || len(pushurllist) < 1 {
		log.Errorf("StopStaticPush: GetStaticPushList error=%v", err)
		return
	}

	for _, pushurl := range pushurllist {
		//pushurl := pushurl + "/" + streamname
		log.Infof("StopStaticPush: static pushurl=%s", pushurl)

		staticpushObj, err := rtmprelay.GetStaticPushObject(pushurl)
		if (staticpushObj != nil) && (err == nil) {
			staticpushObj.Stop()
			rtmprelay.ReleaseStaticPushObject(pushurl)
			log.Infof("StopStaticPush: staticpushObj.Stop %s ", pushurl)
		} else {
			log.Errorf("StopStaticPush GetStaticPushObject %s error", pushurl)
		}
	}
}

func (s *Stream) IsSendStaticPush() bool {
	connTypeList, pushurllist, err := rtmprelay.GetStaticPushList(s.info.URL)
	if err != nil || len(pushurllist) < 1 {
		return false
	}

	for index, pushurl := range pushurllist {
		staticpushObj, err := rtmprelay.GetStaticPushObject(pushurl)
		if (staticpushObj != nil) && (err == nil) {
			return true
		} else {
			log.Errorf("SendStaticPush GetStaticPushObject %s error", pushurl)
			staticpushObj := rtmprelay.GetAndCreateStaticPushObject(connTypeList[index], pushurl)
			if staticpushObj != nil {
				err = staticpushObj.Start()
				if err != nil {
					log.Errorf("restart staticpush error url=%s, error=%v", pushurl, err)
					return false
				}
				log.Warningf("restart staticpush ok url=%s", pushurl)
				return true
			}
		}
	}
	return false
}

func (s *Stream) SendStaticPush(packet av.Packet) {
	connTypeList, pushurllist, err := rtmprelay.GetStaticPushList(s.info.URL)
	if err != nil || len(pushurllist) == 0 {
		return
	}
	cs := &core.ChunkStream{}
	cs.Data = packet.Data
	cs.Length = uint32(len(packet.Data))
	cs.Timestamp = packet.TimeStamp

	if packet.IsVideo {
		cs.TypeID = av.TAG_VIDEO
	} else {
		if packet.IsMetadata {
			cs.TypeID = av.TAG_SCRIPTDATAAMF0
		} else {
			cs.TypeID = av.TAG_AUDIO
		}
	}
	for index, pushurl := range pushurllist {
		staticpushObj, err := rtmprelay.GetStaticPushObject(pushurl)
		if (staticpushObj != nil) && (err == nil) {
			staticpushObj.Send(cs)
		} else {
			log.Errorf("SendStaticPush GetStaticPushObject %s error", pushurl)
			staticpushObj := rtmprelay.GetAndCreateStaticPushObject(connTypeList[index], pushurl)
			if staticpushObj != nil {
				err = staticpushObj.Start()
				if err != nil {
					log.Errorf("restart staticpush error url=%s, error=%v", pushurl, err)
					return
				}
				log.Warningf("restart staticpush ok url=%s", pushurl)
				staticpushObj.Send(cs)
				return
			}
		}
	}
}

func (s *Stream) TransStart() {
	defer func() {
		s.IsPublishFlag = false
		if r := recover(); r != nil {
			log.Errorf("TransStart info(%v) panic:%v", s.info, r)
		}
	}()
	s.isStart = true
	var p av.Packet

	log.Infof("TransStart:%v", s.info)

	s.StartStaticPush()

	for {
		if !s.isStart {
			log.Info("Stream stop: call closeInter", s.info)
			s.closeInter()
			return
		}

		for {
			err := s.r.Read(&p)
			if err != nil {
				log.Error("Stream Read error:", s.info, err)
				s.isStart = false
				s.closeInter()
				return
			}
			break
		}

		if s.IsSendStaticPush() {
			s.SendStaticPush(p)
		}

		s.cache.Write(p)

		if s.ws.IsEmpty() {
			continue
		}

		for item := range s.ws.IterBuffered() {
			v := item.Val.(*PackWriterCloser)
			if !v.init {
				if err := s.cache.Send(v.w); err != nil {
					log.Infof("[%s] send cache packet error: %v, remove", v.w.Info(), err)
					s.ws.Remove(item.Key)
					continue
				}
				v.init = true
			} else {
				new_packet := &av.Packet{}
				*new_packet = p
				if err := v.w.Write(new_packet); err != nil {
					s.ws.Remove(item.Key)
				}
			}
		}
	}
}

func (s *Stream) TransStop() {
	log.Infof("TransStop: %s", s.info.Key)

	if s.isStart && s.r != nil {
		s.r.Close(errors.New("stop old"))
	}

	s.isStart = false
}

func (s *Stream) CheckAlive() (n int) {
	if s.r != nil && s.isStart {
		if s.r.Alive() {
			//log.Infof("Stream.CheckAlive.read Alive ok urlkey=%s", s.info.Key)
			n++
		} else {
			log.Error("Stream.CheckAlive publish error:", s.info.Key)
			s.r.Close(errors.New("read timeout"))
		}
	}
	for item := range s.ws.IterBuffered() {
		v := item.Val.(*PackWriterCloser)
		if v.w != nil {
			log.Debugf("Stream.CheckAlive.write Alive ok urlkey=%s", s.info.Key)
			if !v.w.Alive() {
				s.ws.Remove(item.Key)
				log.Error("Stream.CheckAlive play error:", s.info.Key)
				v.w.Close(errors.New("write timeout"))
				continue
			}
			n++
		}

	}
	return
}

func (s *Stream) ExecPushDone(key string) {
	execList := configure.GetExecPushDone()

	for _, execItem := range execList {
		cmdString := fmt.Sprintf("%s -k %s", execItem, key)
		go func(cmdString string) {
			log.Info("ExecPushDone:", cmdString)
			cmd := exec.Command("/bin/sh", "-c", cmdString)
			_, err := cmd.Output()
			if err != nil {
				log.Info("Excute error:", err)
			}
		}(cmdString)
	}
}

func (s *Stream) closeInter() {
	if s.r != nil {
		if s.IsSendStaticPush() {
			s.StopStaticPush()
		}
		log.Infof("closeInter: [%v] publisher closed", s.r.Info())
	}

	s.ExecPushDone(s.r.Info().Key)
	for item := range s.ws.IterBuffered() {
		v := item.Val.(*PackWriterCloser)
		if v.w != nil {
			if v.w.Info().IsInterval() {
				go func() {
					v.w.Close(errors.New("closed"))
					s.ws.Remove(item.Key)
					log.Infof("[%v] player closed and remove\n", v.w.Info())
				}()
			}
		}
	}
}
