package core

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	neturl "net/url"
	"strings"

	"github.com/livego/av"
	log "github.com/livego/logging"
	"github.com/livego/protocol/amf"
	"time"
)

var (
	respResult     = "_result"
	respError      = "_error"
	onStatus       = "onStatus"
	publishStart   = "NetStream.Publish.Start"
	playStart      = "NetStream.Play.Start"
	connectSuccess = "NetConnection.Connect.Success"
	onBWDone       = "onBWDone"
)

var (
	ErrFail = errors.New("respone err")
)

type ConnClient struct {
	done          bool
	transID       int
	url           string
	tcurl         string
	app           string
	title         string
	query         string
	curcmdName    string
	streamid      uint32
	conn          *Conn
	encoder       *amf.Encoder
	decoder       *amf.Decoder
	bytesw        *bytes.Buffer
	IsStartFlag   bool
}

func NewConnClient() *ConnClient {
	return &ConnClient{
		transID: 1,
		bytesw:  bytes.NewBuffer(nil),
		encoder: &amf.Encoder{},
		decoder: &amf.Decoder{},
	}
}

func (self *ConnClient) GetUrl() string {
	return self.url
}

func (connClient *ConnClient) DecodeBatch(r io.Reader, ver amf.Version) (ret []interface{}, err error) {
	vs, err := connClient.decoder.DecodeBatch(r, ver)
	return vs, err
}

func (connClient *ConnClient) readRespMsg() error {
	var err error
	var rc ChunkStream
	for {
		if err = connClient.conn.Read(&rc); err != nil {
			return err
		}
		if err != nil && err != io.EOF {
			return err
		}
		switch rc.TypeID {
		case 20, 17:
			r := bytes.NewReader(rc.Data)
			vs, _ := connClient.decoder.DecodeBatch(r, amf.AMF0)

			for k, v := range vs {
				switch v.(type) {
				case string:
					switch connClient.curcmdName {
					case cmdConnect, cmdCreateStream:
						if v.(string) != respResult && v.(string) != onBWDone {
							return errors.New(fmt.Sprintf("createstream return unknow type:%s", v.(string)))
						}

					case cmdPublish:
						if v.(string) != onStatus && v.(string) != respResult {
							return errors.New(fmt.Sprintf("publish return unknow type:%s", v.(string)))
						}
					}
				case float64:
					switch connClient.curcmdName {
					case cmdConnect, cmdCreateStream:
						id := int(v.(float64))

						if k == 1 {
							if id != connClient.transID {
								log.Infof("%s return unknow transid:%d, connClient.transID=%d",
									connClient.curcmdName, id, connClient.transID)
							}
						} else if k == 3 {
							connClient.streamid = uint32(id)
						}
					case cmdPublish:
						if int(v.(float64)) != 0 && int(v.(float64)) != 1 && int(v.(float64)) != 2 {
							return errors.New(fmt.Sprintf("publish return unknow float:%f", v.(float64)))
						}
					}
				case amf.Object:
					objmap := v.(amf.Object)
					switch connClient.curcmdName {
					case cmdConnect:
						code, ok := objmap["code"]
						if ok && code.(string) != connectSuccess {
							return errors.New(fmt.Sprintf("connect return unknow code:%s", code.(string)))
						}
					case cmdPublish:
						code, ok := objmap["code"]
						if ok && code.(string) != publishStart {
							return errors.New(fmt.Sprintf("publish return unknow code:%s", code.(string)))
						}
					}
				}
			}

			return nil
		}
	}
}

func (connClient *ConnClient) WriteBaseMeta(version string, fps int, width int, height int, samplerate int, samplesize int) error {
	connClient.bytesw.Reset()

	meta_name := "onMetadata"
	_, err := connClient.encoder.Encode(connClient.bytesw, meta_name, amf.AMF0)
	if err != nil {
		log.Errorf("WriteBaseMeta encode error:%v", err)
		return err
	}
	obj := make(amf.Object)
	obj["version"] = version
	obj["fps"] = fps
	obj["width"] = width
	obj["height"] = height
	obj["audiosamplerate"] = samplerate
	obj["audiosamplesize"] = samplesize
	_, err = connClient.encoder.EncodeAmf0EcmaArray(connClient.bytesw, obj, true)
	if err != nil {
		log.Errorf("WriteBaseMeta EncodeAmf0EcmaArray error:%v", err)
		return err
	}

	msg := connClient.bytesw.Bytes()
	c := ChunkStream{
		Format:    0,
		CSID:      3,
		Timestamp: 0,
		TypeID:    18,
		StreamID:  connClient.streamid,
		Length:    uint32(len(msg)),
		Data:      msg,
	}
	connClient.conn.Write(&c)
	return connClient.conn.Flush()
}

func (connClient *ConnClient) WriteTimestampMeta(timestamp uint32) error {
	//log.Printf("WriteTimestampMeta timestamp=%d", timestamp)
	err := connClient.writeMetaDataMsg(connClient.streamid, "syncTimebase", timestamp)
	if err != nil {
		log.Errorf("WriteTimestampMeta error=%v", err)
	}

	return err
}

func (connClient *ConnClient) writeMetaDataMsg(streamid uint32, args ...interface{}) error {
	connClient.bytesw.Reset()
	for _, v := range args {
		if _, err := connClient.encoder.Encode(connClient.bytesw, v, amf.AMF0); err != nil {
			return err
		}
	}
	msg := connClient.bytesw.Bytes()
	c := ChunkStream{
		Format:    0,
		CSID:      3,
		Timestamp: 0,
		TypeID:    18,
		StreamID:  streamid,
		Length:    uint32(len(msg)),
		Data:      msg,
	}
	connClient.conn.Write(&c)
	return connClient.conn.Flush()
}

func (connClient *ConnClient) writeMsg(args ...interface{}) error {
	connClient.bytesw.Reset()
	for _, v := range args {
		if _, err := connClient.encoder.Encode(connClient.bytesw, v, amf.AMF0); err != nil {
			return err
		}
	}
	msg := connClient.bytesw.Bytes()
	c := ChunkStream{
		Format:    0,
		CSID:      3,
		Timestamp: 0,
		TypeID:    20,
		StreamID:  connClient.streamid,
		Length:    uint32(len(msg)),
		Data:      msg,
	}
	connClient.conn.Write(&c)
	return connClient.conn.Flush()
}

func (connClient *ConnClient) writeConnectMsg() error {
	event := make(amf.Object)
	event["app"] = connClient.app
	event["type"] = "nonprivate"
	event["flashVer"] = "FMS.3.1"
	event["tcUrl"] = connClient.tcurl
	connClient.curcmdName = cmdConnect

	if err := connClient.writeMsg(cmdConnect, connClient.transID, event); err != nil {
		return err
	}
	return connClient.readRespMsg()
}

func (connClient *ConnClient) writeCreateStreamMsg() error {
	connClient.transID++
	connClient.curcmdName = cmdCreateStream

	if err := connClient.writeMsg(cmdCreateStream, connClient.transID, nil); err != nil {
		return err
	}

	for {
		err := connClient.readRespMsg()
		if err == nil {
			return err
		}

		if err == ErrFail {
			log.Errorf("writeCreateStreamMsg readRespMsg err=%v", err)
			return err
		}
	}

}

func (connClient *ConnClient) writePublishMsg() error {
	connClient.transID++
	connClient.curcmdName = cmdPublish
	if err := connClient.writeMsg(cmdPublish, connClient.transID, nil, connClient.title, publishLive); err != nil {
		return err
	}
	return connClient.readRespMsg()
}

func (connClient *ConnClient) writePlayMsg() error {
	connClient.transID++
	connClient.curcmdName = cmdPlay

	if err := connClient.writeMsg(cmdPlay, 0, nil, connClient.title); err != nil {
		return err
	}
	return connClient.readRespMsg()
}

func (connClient *ConnClient) StartOnConn(conn net.Conn, url string, method string) error {
	if connClient.IsStartFlag {
		return errors.New(fmt.Sprintf("ConnClient has already started url=%s", url))
	}
	u, err := neturl.Parse(url)
	if err != nil {
		return err
	}
	connClient.url = url
	path := strings.TrimLeft(u.Path, "/")

	name_list := strings.Split(path, "/")
	if len(name_list) < 2 {
		return fmt.Errorf("u path err: %s", path)
	}
	for index := 0; index < len(name_list)-1; index++ {
		if index == 0 {
			connClient.app = fmt.Sprintf("%s", name_list[index])
		} else {
			connClient.app = fmt.Sprintf("%s/%s", connClient.app, name_list[index])
		}
	}
	connClient.title = name_list[len(name_list)-1]

	connClient.query = u.RawQuery
	connClient.tcurl = "rtmp://" + u.Host + "/" + connClient.app
	port := ":1935"
	host := u.Host
	//localIP := ":0"
	var remoteIP string
	if strings.Index(host, ":") != -1 {
		host, port, err = net.SplitHostPort(host)
		if err != nil {
			return err
		}
		port = ":" + port
	}
	ips, err := net.LookupIP(host)
	log.Infof("ips: %v, host: %v", ips, host)
	if err != nil {
		log.Error(err)
		return err
	}
	remoteIP = ips[rand.Intn(len(ips))].String()
	if strings.Index(remoteIP, ":") == -1 {
		remoteIP += port
	}

	log.Info("connection:", "local:", conn.LocalAddr(), "remote:", conn.RemoteAddr(), "path:", path, "app:", connClient.app, "title:", connClient.title)

	connClient.conn = NewConn(conn, 4*1024)

	log.Info("HandshakeClient....")
	if err := connClient.conn.HandshakeClient(); err != nil {
		return err
	}

	log.Info("writeConnectMsg....")
	if err := connClient.writeConnectMsg(); err != nil {
		return err
	}
	log.Info("writeCreateStreamMsg....")
	if err := connClient.writeCreateStreamMsg(); err != nil {
		log.Error("writeCreateStreamMsg error", err)
		return err
	}

	log.Info("method control:", method, av.PUBLISH, av.PLAY)
	if method == av.PUBLISH {
		if err := connClient.writePublishMsg(); err != nil {
			return err
		}
	} else if method == av.PLAY {
		if err := connClient.writePlayMsg(); err != nil {
			return err
		}
	}

	connClient.IsStartFlag = true
	return nil
}

func (connClient *ConnClient) Start(url string, method string) error {
	if connClient.IsStartFlag {
		return errors.New(fmt.Sprintf("ConnClient has already started url=%s", url))
	}
	u, err := neturl.Parse(url)
	if err != nil {
		return err
	}
	connClient.url = url
	path := strings.TrimLeft(u.Path, "/")

	name_list := strings.Split(path, "/")
	if len(name_list) < 2 {
		return fmt.Errorf("u path err: %s", path)
	}
	for index := 0; index < len(name_list)-1; index++ {
		if index == 0 {
			connClient.app = fmt.Sprintf("%s", name_list[index])
		} else {
			connClient.app = fmt.Sprintf("%s/%s", connClient.app, name_list[index])
		}
	}
	connClient.title = name_list[len(name_list)-1]

	connClient.query = u.RawQuery
	connClient.tcurl = "rtmp://" + u.Host + "/" + connClient.app
	port := ":1935"
	host := u.Host
	//localIP := ":0"
	var remoteIP string
	if strings.Index(host, ":") != -1 {
		host, port, err = net.SplitHostPort(host)
		if err != nil {
			return err
		}
		port = ":" + port
	}
	ips, err := net.LookupIP(host)
	log.Infof("rtmp start connecting(%s) ips: %v, host: %v", url, ips, host)
	if err != nil {
		log.Error(err)
		return err
	}
	remoteIP = ips[rand.Intn(len(ips))].String()
	if strings.Index(remoteIP, ":") == -1 {
		remoteIP += port
	}

	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := d.Dial("tcp", remoteIP)
	//conn, err := net.DialTCP("tcp", local, remote)
	if err != nil {
		log.Error(err)
		return err
	}

	connClient.conn = NewConn(conn, 4*1024)

	if err := connClient.conn.HandshakeClient(); err != nil {
		log.Errorf("rtmp connect HandshakeClient error:%v, url=%s", err, url)
		return err
	}

	if err := connClient.writeConnectMsg(); err != nil {
		log.Errorf("rtmp connect writeConnectMsg error:%v, url=%s", err, url)
		return err
	}

	if err := connClient.writeCreateStreamMsg(); err != nil {
		log.Errorf("rtmp connect writeCreateStreamMsg error:%v, url=%s", err, url)
		return err
	}
	if method == av.PUBLISH {
		if err := connClient.writePublishMsg(); err != nil {
			log.Errorf("rtmp connect writePublishMsg error:%v, url=%s", err, url)
			return err
		}
	} else if method == av.PLAY {
		if err := connClient.writePlayMsg(); err != nil {
			log.Errorf("rtmp connect writePlayMsg error:%v, url=%s", err, url)
			return err
		}
	}

	log.Infof("rtmp connect for %s ok, url=%s", method, url)
	connClient.IsStartFlag = true
	return nil
}

func (connClient *ConnClient) IsStart() bool {
	return connClient.IsStartFlag
}

func (connClient *ConnClient) Write(c ChunkStream) error {
	if c.TypeID == av.TAG_SCRIPTDATAAMF0 ||
		c.TypeID == av.TAG_SCRIPTDATAAMF3 {
		var err error
		if c.Data, err = amf.MetaDataReform(c.Data, amf.ADD); err != nil {
			return err
		}
		c.Length = uint32(len(c.Data))
	}
	return connClient.conn.Write(&c)
}

func (connClient *ConnClient) Read(c *ChunkStream) (err error) {
	return connClient.conn.Read(c)
}

func (connClient *ConnClient) GetInfo() (app string, name string, url string, conn *Conn) {
	app = connClient.app
	name = connClient.title
	url = connClient.url
	conn = connClient.conn
	return
}

func (connClient *ConnClient) GetStreamId() uint32 {
	return connClient.streamid
}

func (connClient *ConnClient) Close(err error) {
	connClient.conn.Close()
}
