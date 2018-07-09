package rtmp

import (
	"github.com/livego/concurrent-map"
	"github.com/livego/configure"
	log "github.com/livego/logging"
	"strings"
	"errors"
	"fmt"
	"time"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"github.com/livego/protocol/rtmp/rtmprelay"
	"net"
)

const PLAY_UPSTREAM_TIMEOUT = 10*1000
const UPSTREAM_MAX_COUNT    = 20*1000

type NODE_INFO struct {
    Nodeip string    `json:"nodeip"`
}

type UPSTREAM_INFO struct {
	Errno int        `json:"errno"`
	Result NODE_INFO `json:"result"`
	Errmsg string    `json:"errmsg"`
}

type UpstreamMgr struct {
	srcRelay cmap.ConcurrentMap //live/liveid, retmrelayobj
	upstreamMap cmap.ConcurrentMap
	timeoutMap cmap.ConcurrentMap
	workChann chan int
	endChann chan int
	rtmpstreamObj *RtmpStream
	startFlag bool
}

func getLocalIp() string {
	var IpAddr string
	addrSlice, err := net.InterfaceAddrs()
	if nil != err {
		log.Error("Get local IP addr failed!!!")
		return ""
	}
	for _, addr := range addrSlice {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if nil != ipnet.IP.To4() {
				IpAddr = ipnet.IP.String()
				return IpAddr
			}
		}
	}
	return ""
}

func NewUpstreaMgr(rtmpstreamObj *RtmpStream) *UpstreamMgr {
	return &UpstreamMgr{
		srcRelay:cmap.New(),
		upstreamMap:cmap.New(),
		timeoutMap:cmap.New(),
		workChann: make(chan int, 2000),
		endChann: make(chan int),
		rtmpstreamObj:rtmpstreamObj,
		startFlag:false,
	}
}

func (self *UpstreamMgr) Notify(keyinfo string) error {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("upstream manager notify panic:%v", r)
		}
	}()

	log.Debugf("UpstreamMgr notify keyinfo=%s", keyinfo)
	startIndex := strings.Index(keyinfo, "/")
	if startIndex < 0 {
		log.Errorf("Notify keyinfo(%s) error", keyinfo)
		return errors.New(fmt.Sprint("Notify keyinfo(%s) error", keyinfo))
	}

	appid := keyinfo[:startIndex]

	url, enable  := configure.GetUpstreamCfg(appid)
	if !enable {
		return nil
	}

	if self.upstreamMap.Has(keyinfo) {
		return nil
	}
	self.upstreamMap.Set(keyinfo, url)

	self.workChann <- 1
	return nil
}

func (self *UpstreamMgr) checkurl(url string, keyinfo string, waitChan chan string) {
	defer func() {
		waitChan <- keyinfo
		if r := recover(); r != nil {
			log.Errorf("checkurl(%s) panic error:%v", url, r)
		}
	}()

	resp, err := http.Get(url)
	if err != nil {
		log.Errorf("checkurl url=%s, error=%v", url, err)
		return
	}
	defer resp.Body.Close()

	var info UPSTREAM_INFO
	body, err := ioutil.ReadAll(resp.Body)

	err = json.Unmarshal(body, &info)
	if err != nil {
		log.Errorf("checkurl json unmarshal(%s) error:%v", string(body), err)
		return
	}

	if info.Errno != 200 {
		log.Errorf("checkurl(%s) return %d:%s", url, info.Errno, info.Errmsg)
		value, isExist := self.srcRelay.Get(keyinfo)
		if isExist {
			relayObj := value.(*rtmprelay.RtmpRelay)
			relayObj.Stop()
			log.Warningf("checkurl remove rtmprelay for url(%s) no info in cluster", url)
			self.srcRelay.Remove(keyinfo)
		}
		if _, isExist := self.timeoutMap.Get(keyinfo); isExist {
			self.timeoutMap.Remove(keyinfo)
			log.Warningf("checkurl timeout map: remove %s", keyinfo)
		}
		if isExist := self.upstreamMap.Has(keyinfo); isExist {
			self.upstreamMap.Remove(keyinfo)
			log.Warningf("checkurl upstream map: remove %s", keyinfo)
		}
		return
	}
	log.Debugf("checkurl(%s) return %s", string(body))
	localIP := getLocalIp()
	if info.Result.Nodeip == localIP {
		log.Errorf("checkurl(%s) nodeip(%s) is localip", url, info.Result.Nodeip)
		return
	}

	playUrl := fmt.Sprintf("rtmp://%s/ali-bj-bgp.pull.inke.cn/%s", info.Result.Nodeip, keyinfo)
	publishUrl := fmt.Sprintf("rtmp://127.0.0.1/%s", keyinfo)

	log.Infof("checkurl get upstream playurl=%s, publishurl=%s", playUrl, publishUrl)

	if !self.srcRelay.Has(keyinfo) {
        relayObj := rtmprelay.NewRtmpRelay(&playUrl, &publishUrl)
        err = relayObj.Start()
        if err != nil {
        	log.Errorf("checkurl rtmprelay start error:%v", err)
        	return
		}
		self.srcRelay.Set(keyinfo, relayObj)
	} else {
		value, isExist := self.srcRelay.Get(keyinfo)
		if isExist {
			relayObj := value.(*rtmprelay.RtmpRelay)
			if relayObj.PlayUrl != playUrl {
				log.Warningf("checkurl sourcurl change from %s to %s", relayObj.PlayUrl, playUrl)
				relayObj.Stop()
                self.srcRelay.Remove(keyinfo)
				relayObj := rtmprelay.NewRtmpRelay(&playUrl, &publishUrl)
				err = relayObj.Start()
				if err != nil {
					log.Errorf("checkurl rtmprelay start error:%v", err)
					return
				}
				self.srcRelay.Set(keyinfo, relayObj)
			}
		}
	}
}

func (self *UpstreamMgr) checkSourcestream() {
	count := 0
    waitChann := make(chan string, UPSTREAM_MAX_COUNT)
    var deleteList []string

	for item := range self.upstreamMap.IterBuffered() {
        keyinfo := item.Key
        upstreamUrl := item.Val.(string)

        isExist := self.rtmpstreamObj.GetStreams().Has(keyinfo)
        if !isExist {
			deleteList = append(deleteList, keyinfo)
		} else {
			//check live cluster whether there is the livestream
			startIndex := strings.Index(keyinfo, "/")
			liveid := keyinfo[startIndex+1:]
			url := fmt.Sprintf("%s%s", upstreamUrl, liveid)
			log.Infof("checkSourcestream url=%s", url)
			count++
			go self.checkurl(url, keyinfo, waitChann)
		}
	}

	for _, deleteItem := range deleteList {
		if self.upstreamMap.Has(deleteItem) {
			log.Warningf("checkSourcestream upstreamMap remove %s", deleteItem)
			self.upstreamMap.Remove(deleteItem)
		}
        if self.timeoutMap.Has(deleteItem) {
			log.Warningf("checkSourcestream timeoutMap remove %s", deleteItem)
        	self.timeoutMap.Remove(deleteItem)
		}
		if value, isExist := self.srcRelay.Get(deleteItem); isExist {
			relayObj := value.(*rtmprelay.RtmpRelay)
			log.Warningf("checkSourcestream srcRelay remove %s", deleteItem)
			relayObj.Stop()
			self.srcRelay.Remove(deleteItem)
		}
	}

	for index := 0; index < count; index++ {
		select {
		case retKey, ok := <- waitChann:
			if ok {
				log.Debugf("checkurl keyinfo=%s is over", retKey)
			}
			break
		case <- time.After(300*time.Millisecond):
			log.Warningf("checkurl is timeout")
			break
		}
	}

	close(waitChann)
}

func (self *UpstreamMgr) checkLocalstream() {
    for item := range self.rtmpstreamObj.GetStreams().IterBuffered() {
    	keyinfo := item.Key
    	streamObj := item.Val.(*Stream)

    	playCount := streamObj.GetWs().Count()
    	log.Debugf("checkLocalstream key=%s, playcount=%d", keyinfo, playCount)
    	if playCount == 0 {
			nowt := time.Now().UnixNano() / (1000*1000)
    		if value, isExist := self.timeoutMap.Get(keyinfo); isExist {
    			timestamp := value.(int64)
    			if nowt - timestamp < PLAY_UPSTREAM_TIMEOUT {
    				log.Debugf("checkLocalstream timeout=%d, keyinfo=%s", nowt - timestamp, keyinfo)
    				continue
				}
				log.Warningf("checkLocalstream timeout=%d, keyinfo=%s", nowt - timestamp, keyinfo)
			} else {
				self.timeoutMap.Set(keyinfo, nowt)
				continue
			}
			if value, isExist := self.srcRelay.Get(keyinfo); isExist {
                relayObj := value.(*rtmprelay.RtmpRelay)
                relayObj.Stop()
                self.srcRelay.Remove(keyinfo)
                log.Warningf("rtmprelay map: remove %s", keyinfo)
				if _, isExist := self.timeoutMap.Get(keyinfo); isExist {
					self.timeoutMap.Remove(keyinfo)
					log.Warningf("timeout map: remove %s", keyinfo)
				}
				if isExist := self.upstreamMap.Has(keyinfo); isExist {
					self.upstreamMap.Remove(keyinfo)
					log.Warningf("upstream map: remove %s", keyinfo)
				}
			}
		} else {
			if _, isExist := self.timeoutMap.Get(keyinfo); isExist {
				self.timeoutMap.Remove(keyinfo)
			}
		}
	}
}

func (self *UpstreamMgr) onwork() {
	defer func() {
		self.endChann <- 1
		if r := recover(); r != nil {
			log.Errorf("upstream manager onwork is over, panic=%v", r)
		}
	}()
    for {
    	if ret := self.wait(); !ret {
    		log.Warningf("upstream mananger onwork is over.")
    		break
		}
		if !self.startFlag {
			break
		}
        self.checkSourcestream()

        self.checkLocalstream()
	}
}

func (self *UpstreamMgr) wait() (ret bool) {
	ret = false

	select {
	case _, ok := <- self.workChann:
		ret = ok
		break
	case <- time.After(3000*time.Millisecond):
		ret = true
		break
	}
	return
}

func (self *UpstreamMgr) Start() error {
	if self.startFlag {
		return errors.New("UpstreamMgr has already started.")
	}
	self.startFlag = true
	go self.onwork()
	return nil
}

func (self *UpstreamMgr) Stop() {
	if !self.startFlag {
		return
	}
	self.startFlag = false
	close(self.workChann)
	<- self.endChann
	close(self.endChann)
}