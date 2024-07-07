package filequeue

import "os"

func Delete(path string) {
	l.Lock()
	defer l.Unlock()
	if _, ok := QueueMap[path]; ok {
		if QueueMap[path].writeInfo != nil {
			QueueMap[path].writeInfo.fd.Close()
		}

		if QueueMap[path].readInfo != nil {
			QueueMap[path].readInfo.fd.Close()
		}
	}
	os.Remove(path)
}
