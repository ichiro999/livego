package rtmprelay

import (
	"errors"
	"fmt"
	log "github.com/livego/logging"
)

var (
	STOP_CTRL = "RTMPRELAY_STOP"
)

type RtmpRelay struct {
	PlayUrl              string
	PublishUrl           string
	startflag            bool
    rtmppull             *RtmpPull
    rtmppush             *RtmpPush
}

func NewRtmpRelay(playurl *string, publishurl *string) *RtmpRelay {
	return &RtmpRelay{
		PlayUrl:    *playurl,
		PublishUrl: *publishurl,
		startflag:  false,
		rtmppull:   NewRtmpPull(*playurl),
		rtmppush:   NewRtmpPush(*publishurl),
	}
}


func (self *RtmpRelay) IsStart() bool {
	return self.startflag
}

func (self *RtmpRelay) Start() error {
	if self.startflag {
		err := errors.New(fmt.Sprintf("The rtmprelay already started, playurl=%s, publishurl=%s", self.PlayUrl, self.PublishUrl))
		return err
	}

    self.rtmppull.SetWriter(self.rtmppush)

    err := self.rtmppull.Start()
    if err != nil {
    	return err
	}

	err = self.rtmppush.Start()
	if err != nil {
		self.rtmppull.Stop()
		return err
	}

	self.startflag = true
	return nil
}

func (self *RtmpRelay) Stop() {
	if !self.startflag {
		log.Errorf("The rtmprelay already stoped, playurl=%s, publishurl=%s", self.PlayUrl, self.PublishUrl)
		return
	}

	self.rtmppull.Stop()
	self.rtmppush.Stop()
	self.startflag = false
}
