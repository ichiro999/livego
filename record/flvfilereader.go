package record

import (
	"os"
	"errors"
	"fmt"
	"github.com/livego/utils/pio"
	log "github.com/livego/logging"
)

var FILE_FLV_HEADER = [13]byte{0x46, 0x4c, 0x56, 0x01, 0x05, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00}

type FlvFileReader struct {
	flvFilename string
	isOpen bool
	filehandle *os.File
	currentPos int64
}

func NewFlvFileReader(filename string) *FlvFileReader {
	return &FlvFileReader{
		flvFilename:filename,
		isOpen: false,
		filehandle: nil,
		currentPos: 0,
	}
}

func (self *FlvFileReader) Open() error {
    if self.isOpen {
    	return errors.New(fmt.Sprintf("filename %s is already opened", self.flvFilename))
	}

	ret := checkFileIsExist(self.flvFilename)
	if !ret {
		return errors.New(fmt.Sprintf("filename %s not exist", self.flvFilename))
	}

	var err error
	self.filehandle, err = os.Open(self.flvFilename)
	if err != nil {
		log.Error("Open flv error:", err)
		return err
	}
	self.currentPos = 0
	headerData := make([]byte, len(FILE_FLV_HEADER))

	count, err := self.filehandle.Read(headerData)
	if err != nil || count != len(FILE_FLV_HEADER) {
		return errors.New(fmt.Sprintf("filename read header error:%v", err))
	}

	self.currentPos += int64(count)
	self.isOpen = true

	return nil
}

func (self *FlvFileReader) Close() {
	if !self.isOpen || self.filehandle == nil {
		return
	}

    self.filehandle.Close()
    self.filehandle = nil
    self.isOpen = false
    self.currentPos = 0
}
func (self *FlvFileReader) Read() (err error, mediaData []byte, timestamp int, mediaType int) {
    //| 1B |    3B        |    4B   |   3B   |   nB       |   4B   |
	//|type|payload length|timestamp|streamid|payload data|pre size|
	const PRE_SIZE = 4
	var ret int
	itemHeader := make([]byte, ITEM_HEADER_LEN)

	ret, err = self.filehandle.ReadAt(itemHeader, self.currentPos)
	if err != nil || ret != ITEM_HEADER_LEN {
		return
	}

	self.currentPos += int64(ret)

	mediaType = int(pio.U8(itemHeader[:]))
	dataLen := int(pio.U24BE(itemHeader[1:]))
	timeData := itemHeader[4:8]
    timestamp = (int(timeData[3]) << 24) | (int(timeData[0]) << 16) | (int(timeData[1]) << 8) | int(timeData[2])

	mediaData = make([]byte, dataLen)
	ret, err = self.filehandle.ReadAt(mediaData, self.currentPos)
	if err != nil || ret != dataLen {
		return
	}

	self.currentPos += int64(ret)
	self.currentPos += PRE_SIZE
	//log.Infof("read flv: type=%d, timestamp=%d, len=%d", mediaType, timestamp, len(mediaData))
	return
}