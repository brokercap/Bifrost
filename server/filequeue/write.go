package filequeue

import "log"

const FileMaxSize int64 = 16 * 1024 * 1024

func (This *Queue) Append(content string) error {
	return This.AppendBytes([]byte(content))
}

func (This *Queue) AppendBytes(b []byte) error {
	This.Lock()
	defer This.Unlock()
	if This.writeInfo == nil {
		This.writeInfoInit()
	}
	if This.writeInfo.pos > 0 {
		This.writeInfo.fd.Write([]byte(";"))
		This.writeInfo.pos += 1
	}
	n := Int32ToBytes(int32(len(b)))
	_, err0 := This.writeInfo.fd.Write(n)
	if err0 != nil {
		log.Fatal(err0)
	}
	This.writeInfo.fd.Write(b)
	This.writeInfo.fd.Write(n)
	This.writeInfo.pos += 8 + int64(len(b))

	if This.writeInfo.pos >= FileMaxSize {
		//log.Println(This.writeInfo.name," pos:",This.writeInfo.pos)
		This.writeInfo.fd.Close()
		This.writeInfo = nil
	}

	return nil
}
