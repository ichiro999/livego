package configure

import (
	"encoding/json"
	"fmt"
	log "github.com/livego/logging"
	"io/ioutil"
	"strings"
)

/*
{
    "listen": 1935,
    "hls" : "enable"
    "hlsport" : 80
    "httpflv" : "enable"
    "flvport" : 80
    "httpoper": "enable"
    "operport": 80
    "servers":[
        {
        "servername":"live",
        "exec_push":["./helloworld1", "./helloworld2"],
        "exec_push_done":["./helloworld1", "./helloworld2"],
        "report":["127.0.0.1"],
	    "record":[{"master_prefix":"live", "type":"flv",
                   "path":"/Users/xxxx/Documents/record"}]
        }
    ]
}
*/

var (
	CONN_TCP_TYPE = "tcp"
	CONN_QUIC_TYPE = "quic"
)

type RecordConfig struct {
	Master_prefix string `json:"master_prefix"`
	Recordtype    string `json:"type"`
	Path          string `json:"path"`
}

type StaticPushInfo struct {
	ConnType      string
	Master_prefix string
	Upstream      string
}

type StaticPullInfo struct {
	Type   string
	Source string
	App    string
	Stream string
}

type ServerInfo struct {
	Servername      string
	Exec_push       []string
	Exec_push_done  []string
	Report          []string
	Static_push     []StaticPushInfo
	Static_pull     []StaticPullInfo
	Recordcfg       []RecordConfig `json:"record"`
	Upstream_enable string `json:"upstream_enable"`
	Upstream_url    string `json:"upstream_url"`
}

type ServerCfg struct {
	Listen    int
	Hls       string
	Hlsport   int
	Httpflv   string
	Flvport   int
	Httpoper  string
	Operport  int
	Chunksize int
	Servers   []ServerInfo
}

var RtmpServercfg ServerCfg

var isStaticPushEnable bool
var isSubStaticPushEnable bool

func LoadConfig(configfilename string) error {
	log.Infof("starting load configure file(%s)......", configfilename)
	data, err := ioutil.ReadFile(configfilename)
	if err != nil {
		log.Errorf("ReadFile %s error:%v", configfilename, err)
		return err
	}

	log.Infof("loadconfig: \r\n%s", string(data))

	err = json.Unmarshal(data, &RtmpServercfg)
	if err != nil {
		log.Errorf("json.Unmarshal error:%v", err)
		return err
	}
	log.Infof("get config json data:%v", RtmpServercfg)

	if RtmpServercfg.Chunksize == 0 {
		RtmpServercfg.Chunksize = 4096
	}

	log.Warning("Chunk size:", RtmpServercfg.Chunksize)

	return nil
}

func GetRecordCfg() (retList []RecordConfig) {
	retList = nil

	for _, serverItem := range RtmpServercfg.Servers {
		if serverItem.Recordcfg == nil || len(serverItem.Recordcfg) == 0 {
			continue
		}
		retList = append(retList, serverItem.Recordcfg...)
	}
	return
}

func IsRecordEnable(publishUrl string) (bool, RecordConfig) {
	var recCfg RecordConfig
	isEnable := false
	for _, serverItem := range RtmpServercfg.Servers {
		for _, recItem := range serverItem.Recordcfg {
			if strings.Contains(publishUrl, recItem.Master_prefix) {
				isEnable = true
				recCfg = recItem
				break
			}

		}
		break
	}
	return isEnable, recCfg
}

func GetUpstreamCfg(appid string) (upstreamUrl string, enable bool) {
    enable = false

    for _, serverItem := range RtmpServercfg.Servers {
    	if serverItem.Servername == appid {
    		upstreamUrl = serverItem.Upstream_url
    		if serverItem.Upstream_enable == "enable" {
				enable = true
			}
		}
	}
    return
}

func GetReportList() []string {
	var reportlist []string

	for _, serverItem := range RtmpServercfg.Servers {
		reportlist = append(reportlist, serverItem.Report...)
	}

	return reportlist
}

func GetExecPush() []string {
	var execList []string

	for _, serverItem := range RtmpServercfg.Servers {
		for _, item := range serverItem.Exec_push {
			execList = append(execList, item)
		}
	}
	return execList
}

func GetExecPushDone() []string {
	var execList []string

	for _, serverItem := range RtmpServercfg.Servers {
		for _, item := range serverItem.Exec_push_done {
			execList = append(execList, item)
		}
	}
	return execList
}

func GetChunkSize() int {
	if RtmpServercfg.Chunksize > 0 {
		return RtmpServercfg.Chunksize
	}
	RtmpServercfg.Chunksize = 4096
	return RtmpServercfg.Chunksize
}

func IsHttpOperEnable() bool {
	httpOper := strings.ToLower(RtmpServercfg.Httpoper)
	//log.Warning("http operation", httpOper)
	if httpOper == "enable" {
		return true
	}
	return false
}

func IsHttpFlvEnable() bool {
	flv := strings.ToLower(RtmpServercfg.Httpflv)
	//log.Warning("http-flv", flv)
	if flv == "enable" {
		return true
	}
	return false
}

func IsHlsEnable() bool {
	hls := strings.ToLower(RtmpServercfg.Hls)
	//log.Warning("HLS", hls)
	if hls == "enable" {
		return true
	}

	return false
}

func GetListenPort() int {
	return RtmpServercfg.Listen
}

func GetHlsPort() int {
	return RtmpServercfg.Hlsport
}

func GetHttpFlvPort() int {
	return RtmpServercfg.Flvport
}

func GetHttpOperPort() int {
	return RtmpServercfg.Operport
}

func GetStaticPullList() (pullInfoList []StaticPullInfo, bRet bool) {
	pullInfoList = nil
	bRet = false

	for _, serverinfo := range RtmpServercfg.Servers {
		if serverinfo.Static_pull != nil && len(serverinfo.Static_pull) > 0 {
			bRet = true
			pullInfoList = append(pullInfoList, serverinfo.Static_pull[:]...)
		}
	}

	return
}

func GetStaticPushUrlList(rtmpurl string) (connTypeArray []string, retArray []string, bRet bool) {
	if !isStaticPushEnable {
		return nil, nil,false
	}

	retArray = nil
	bRet = false

	if len(rtmpurl) <= 7 {
		log.Errorf("rtmp url error, url=%s", rtmpurl)
		return
	}
	//log.Printf("rtmpurl=%s", rtmpurl)
	url := rtmpurl[7:]

	index := strings.Index(url, "/")
	if index <= 0 {
		return
	}
	url = url[index+1:]
	//log.Printf("GetStaticPushUrlList: url=%s", url)
	for _, serverinfo := range RtmpServercfg.Servers {
		//log.Printf("server info:%v", serverinfo)
		for _, staticpushItem := range serverinfo.Static_push {
			masterPrefix := staticpushItem.Master_prefix
			upstream := staticpushItem.Upstream
			//log.Printf("push item: masterprefix=%s, upstream=%s", masterPrefix, upstream)
			if strings.Contains(url, masterPrefix) {
				newUrl := ""
				index := strings.Index(url, "/")
				if index <= 0 {
					newUrl = url
				} else {
					newUrl = url[index+1:]
				}
				destUrl := fmt.Sprintf("%s/%s/%s", upstream, masterPrefix, newUrl)
				retArray = append(retArray, destUrl)
				if len(staticpushItem.ConnType) == 0 {
					connTypeArray = append(connTypeArray, CONN_TCP_TYPE)
				} else {
					connTypeArray = append(connTypeArray, CONN_QUIC_TYPE)
				}
				bRet = true
			}
		}
	}

	return
}
