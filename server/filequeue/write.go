package filequeue

import "log"

func (This *Queue) Append(content string) error{
	This.Lock()
	defer This.Unlock()
	if This.writeInfo == nil{
		This.writeInfoInit()
		This.noData = false
	}
	if This.writeInfo.pos > 0 {
		This.writeInfo.fd.Write([]byte(";"))
		This.writeInfo.pos+=1
	}
	b := []byte(content)
	n := Int32ToBytes(int32(len(b)))
	_,err0 := This.writeInfo.fd.Write(n)
	if err0 != nil{
		log.Fatal(err0)
	}
	This.writeInfo.fd.Write(b)
	This.writeInfo.fd.Write(n)
	This.writeInfo.pos += (8+int64(len(b)))

	if This.writeInfo.pos >= 16 * 1024 * 8{
		This.writeInfo.fd.Close()
		This.writeInfo = nil
	}

	return nil
}

func (This *Queue) AppendBytes(b []byte) error{
	This.Lock()
	defer This.Unlock()
	if This.writeInfo == nil{
		This.writeInfoInit()
		This.noData = false
	}
	if This.writeInfo.pos > 0 {
		This.writeInfo.fd.Write([]byte(";"))
		This.writeInfo.pos+=1
	}
	n := Int32ToBytes(int32(len(b)))
	_,err0 := This.writeInfo.fd.Write(n)
	if err0 != nil{
		log.Fatal(err0)
	}
	This.writeInfo.fd.Write(b)
	This.writeInfo.fd.Write(n)
	This.writeInfo.pos += (8+int64(len(b)))

	if This.writeInfo.pos >= 16 * 1024 * 8{
		This.writeInfo.fd.Close()
		This.writeInfo = nil
	}

	return nil
}

