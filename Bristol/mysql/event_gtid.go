package mysql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/satori/go.uuid"
	"strings"
)

type GTIDEvent struct {
	header     EventHeader
	CommitFlag uint8
	SID36      string
	GNO        int64
}

func (parser *eventParser) parseGTIDEvent(buf *bytes.Buffer) (event *GTIDEvent, err error) {
	event = new(GTIDEvent)
	err = binary.Read(buf, binary.LittleEndian, &event.header)
	err = binary.Read(buf, binary.LittleEndian, &event.CommitFlag)
	event.SID36 = parser.decodeUuid(buf)
	err = binary.Read(buf, binary.LittleEndian, &event.GNO)
	return
}

type PreviousGTIDSEvent struct {
	header   EventHeader
	GTIDSets string
}

func (parser *eventParser) parsePrevtiousGTIDSEvent(buf *bytes.Buffer) (event *PreviousGTIDSEvent, err error) {
	event = new(PreviousGTIDSEvent)
	err = binary.Read(buf, binary.LittleEndian, &event.header)
	var uuidCount uint8
	binary.Read(buf, binary.LittleEndian, &uuidCount)
	var previousGTIDSets []string
	var lastPrevtiousGTIDSMap = make(map[string]Intervals, 0)
	for i := uint8(0); i < uuidCount; i++ {
		uuid := parser.decodeUuid(buf)
		var sliceCount uint8
		binary.Read(buf, binary.LittleEndian, &sliceCount)
		var intervalArr []string
		var intervals Intervals
		for i := uint8(0); i < sliceCount; i++ {
			start := parser.decodeInterval(buf)
			stop := parser.decodeInterval(buf)
			var interval = fmt.Sprintf("%d-%d", start, stop)
			/*
				if stop == start+1 {
					interval = fmt.Sprintf("%d", start)
				} else {
					interval = fmt.Sprintf("%d-%d", start, stop-1)
				}
			*/
			intervals = Intervals{
				Start: int64(start),
				Stop:  int64(stop),
			}
			intervalArr = append(intervalArr, interval)
		}
		previousGTIDSets = append(previousGTIDSets, fmt.Sprintf("%s:%s", uuid, strings.Join(intervalArr, ":")))
		lastPrevtiousGTIDSMap[uuid] = intervals
	}
	event.GTIDSets = fmt.Sprintf("%s", strings.Join(previousGTIDSets, ","))
	parser.lastPrevtiousGTIDSMap = lastPrevtiousGTIDSMap
	return
}

func (parser *eventParser) decodeUuid(buf *bytes.Buffer) string {
	u, _ := uuid.FromBytes(buf.Next(16))
	return u.String()
}

func (parser *eventParser) decodeInterval(buf *bytes.Buffer) (interval uint8) {
	binary.Read(buf, binary.LittleEndian, &interval)
	return
}
