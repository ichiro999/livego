package rtmprelay

import (
	"errors"
	"fmt"
	"github.com/livego/configure"
	log "github.com/livego/logging"
	"sync"
	"github.com/livego/av"
)

var G_StaticPushMap = make(map[string](Writer))
var g_MapLock = new(sync.RWMutex)

func GetStaticPushList(url string) ([]string, []string, error) {
	connTypeList, pushurlList, ok := configure.GetStaticPushUrlList(url)

	if !ok {
		return nil, nil, errors.New("no static push url")
	}

	return connTypeList, pushurlList, nil
}

func GetAndCreateStaticPushObject(connType string, rtmpurl string) Writer {
	g_MapLock.RLock()
	staticpush, ok := G_StaticPushMap[rtmpurl]
	log.Infof("GetAndCreateStaticPushObject: %s, return %v", rtmpurl, ok)
	if !ok {
		g_MapLock.RUnlock()
		var newStaticpush *RtmpPush
		if len(connType) == 0 || connType == av.TCP_CONNTYPE{
			newStaticpush = NewRtmpPush(rtmpurl)

			g_MapLock.Lock()
			G_StaticPushMap[rtmpurl] = newStaticpush
			g_MapLock.Unlock()

			return newStaticpush
		}
	}
	g_MapLock.RUnlock()

	return staticpush
}

func GetStaticPushObject(rtmpurl string) (Writer, error) {
	g_MapLock.RLock()
	if staticpush, ok := G_StaticPushMap[rtmpurl]; ok {
		g_MapLock.RUnlock()
		return staticpush, nil
	}
	g_MapLock.RUnlock()

	return nil, errors.New(fmt.Sprintf("G_StaticPushMap[%s] not exist...."))
}

func ReleaseStaticPushObject(rtmpurl string) {
	g_MapLock.RLock()
	if _, ok := G_StaticPushMap[rtmpurl]; ok {
		g_MapLock.RUnlock()

		log.Infof("ReleaseStaticPushObject %s ok", rtmpurl)
		g_MapLock.Lock()
		delete(G_StaticPushMap, rtmpurl)
		g_MapLock.Unlock()
	} else {
		g_MapLock.RUnlock()
		log.Errorf("ReleaseStaticPushObject: not find %s", rtmpurl)
	}
}
