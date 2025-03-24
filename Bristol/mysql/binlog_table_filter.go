package mysql

import (
	"log"
	"regexp"
	"strings"
)

func (This *BinlogDump) AddReplicateDoDb(db string, table string) {
	log.Println("Bristol AddReplicateDoDb,", db, table)
	This.Lock()
	defer This.Unlock()
	This.replicateDoDbCheck = true
	if This.ReplicateDoDb == nil {
		This.ReplicateDoDb = make(map[string]map[string]uint8, 0)
	}
	if _, ok := This.ReplicateDoDb[db]; !ok {
		This.ReplicateDoDb[db] = make(map[string]uint8, 0)
	}
	if This.ReplicateDoDbLike == nil {
		This.ReplicateDoDbLike = make(map[string]map[string]uint8, 0)
	}
	if db != "*" {
		if table != "*" && strings.Index(table, "*") >= 0 {
			if _, ok := This.ReplicateDoDbLike[db]; !ok {
				This.ReplicateDoDbLike[db] = make(map[string]uint8, 0)
			}
			This.ReplicateDoDbLike[db][table] = 1
			reqTagAll, err := regexp.Compile(table)
			if err != nil {
				return
			}
			for k, _ := range This.ReplicateDoDb[db] {
				if reqTagAll.FindString(k) != "" {
					This.addReplicateDoDb0(db, k)
				}
			}
			return
		}
	}
	if table != "" {
		This.addReplicateDoDb0(db, table)
	}
}

func (This *BinlogDump) addReplicateDoDb0(db string, table string) {
	if _, ok := This.ReplicateDoDb[db][table]; !ok {
		This.ReplicateDoDb[db][table] = 1
	} else {
		This.ReplicateDoDb[db][table]++
	}
}

func (This *BinlogDump) delReplicateDoDb0(db string, table string) {
	if _, ok := This.ReplicateDoDb[db]; !ok {
		return
	}
	if _, ok := This.ReplicateDoDb[db][table]; ok {
		This.ReplicateDoDb[db][table]--
		if This.ReplicateDoDb[db][table] == 0 {
			delete(This.ReplicateDoDb[db], table)
		}
	}
	if len(This.ReplicateDoDb[db]) == 0 {
		delete(This.ReplicateDoDb, db)
	}
}

func (This *BinlogDump) DelReplicateDoDb(db string, table string) {
	log.Println("Bristol DelReplicateDoDb,", db, table)
	This.Lock()
	defer This.Unlock()
	if table != "*" && strings.Index(table, "*") >= 0 {
		if This.ReplicateDoDbLike != nil {
			if _, ok := This.ReplicateDoDbLike[db]; ok {
				delete(This.ReplicateDoDbLike[db], table)
				reqTagAll, err := regexp.Compile(table)
				if err == nil {
					for k, _ := range This.ReplicateDoDb[db] {
						if len(reqTagAll.FindString(k)) > 0 {
							This.delReplicateDoDb0(db, k)
						}
					}
				}
			}
		}
	} else {
		if This.ReplicateDoDb != nil {
			This.delReplicateDoDb0(db, table)
		}
	}
	if table == "" {
		delete(This.ReplicateDoDbLike, db)
	}
	if (This.ReplicateDoDbLike != nil && len(This.ReplicateDoDbLike) > 0) || (This.ReplicateDoDb != nil && len(This.ReplicateDoDb) > 0) {
		This.replicateDoDbCheck = true
	} else {
		This.replicateDoDbCheck = false
	}
}

func (This *BinlogDump) AddReplicateIgnoreDb(db string, table string) {
	This.Lock()
	defer This.Unlock()
	//不能过滤所有库
	if db == "*" {
		return
	}
	This.replicateIgnoreDbCheck = true
	if This.ReplicateIgnoreDb == nil {
		This.ReplicateIgnoreDb = make(map[string]map[string]uint8, 0)
	}
	if _, ok := This.ReplicateIgnoreDb[db]; !ok {
		This.ReplicateIgnoreDb[db] = make(map[string]uint8, 0)
	}
	if This.ReplicateIgnoreDbLike == nil {
		This.ReplicateIgnoreDbLike = make(map[string]map[string]uint8, 0)
	}
	if db != "*" {
		if table != "*" && strings.Index(table, "*") >= 0 {
			if _, ok := This.ReplicateIgnoreDbLike[db]; !ok {
				This.ReplicateIgnoreDbLike[db] = make(map[string]uint8, 0)
			}
			This.ReplicateIgnoreDbLike[db][table] = 1
			reqTagAll, err := regexp.Compile(table)
			if err != nil {
				return
			}
			for k, _ := range This.ReplicateIgnoreDb[db] {
				if reqTagAll.FindString(k) != "" {
					This.addReplicateIgnoreDb0(db, k)
				}
			}
			return
		}
	}
	if table != "" {
		This.addReplicateIgnoreDb0(db, table)
	}
}

func (This *BinlogDump) addReplicateIgnoreDb0(db string, table string) {
	if _, ok := This.ReplicateIgnoreDb[db][table]; !ok {
		This.ReplicateIgnoreDb[db][table] = 1
	} else {
		This.ReplicateIgnoreDb[db][table]++
	}
}

func (This *BinlogDump) delReplicateIgnoreDb0(db string, table string) {
	if _, ok := This.ReplicateIgnoreDb[db]; !ok {
		return
	}
	if _, ok := This.ReplicateIgnoreDb[db][table]; ok {
		This.ReplicateIgnoreDb[db][table]--
		if This.ReplicateIgnoreDb[db][table] == 0 {
			delete(This.ReplicateIgnoreDb[db], table)
		}
	}
	if len(This.ReplicateIgnoreDb[db]) == 0 {
		delete(This.ReplicateIgnoreDb, db)
	}
}

func (This *BinlogDump) DelReplicateIgnoreDb(db string, table string) {
	log.Println("Bristol DelReplicateIgnoreDb,", db, table)
	This.Lock()
	defer This.Unlock()
	if table != "*" && strings.Index(table, "*") >= 0 {
		if This.ReplicateIgnoreDbLike != nil {
			if _, ok := This.ReplicateIgnoreDbLike[db]; ok {
				delete(This.ReplicateIgnoreDbLike[db], table)
				reqTagAll, err := regexp.Compile(table)
				if err == nil {
					for k, _ := range This.ReplicateIgnoreDb[db] {
						if len(reqTagAll.FindString(k)) > 0 {
							This.delReplicateIgnoreDb0(db, k)
						}
					}
				}
			}
		}
	} else {
		if This.ReplicateIgnoreDb != nil {
			This.delReplicateIgnoreDb0(db, table)
		}
	}

	if table == "" {
		delete(This.ReplicateIgnoreDb, db)
	}

	if (This.ReplicateIgnoreDbLike != nil && len(This.ReplicateIgnoreDbLike) > 0) || (This.ReplicateIgnoreDb != nil && len(This.ReplicateIgnoreDb) > 0) {
		This.replicateIgnoreDbCheck = true
	} else {
		This.replicateIgnoreDbCheck = false
	}
}

func (This *BinlogDump) CheckReplicateDb(db string, table string) bool {
	This.Lock()
	defer This.Unlock()
	if This.ReplicateDoDb == nil && This.ReplicateIgnoreDb == nil {
		return true
	}
	var ok bool
	if This.replicateDoDbCheck {
		if _, ok = This.ReplicateDoDb["*"]; ok {
			return true
		}
		if _, ok = This.ReplicateDoDb[db]; ok {
			if _, ok = This.ReplicateDoDb[db][table]; ok {
				return true
			}
			if _, ok = This.ReplicateDoDb[db]["*"]; ok {
				return true
			}
			if _, ok = This.ReplicateDoDbLike[db]; ok {
				for k, _ := range This.ReplicateDoDbLike[db] {
					reqTagAll, err := regexp.Compile(k)
					if err != nil {
						log.Println("ReplicateDoDbLike reqTagAll err:", err)
						continue
					}
					if reqTagAll.FindString(table) != "" {
						This.addReplicateDoDb0(db, table)
						return true
					}
				}
			}
		}
		return false
	}
	if This.replicateIgnoreDbCheck {
		if _, ok = This.ReplicateIgnoreDb[db]; ok {
			if _, ok = This.ReplicateIgnoreDb[db][table]; ok {
				return true
			}
			if _, ok = This.ReplicateIgnoreDb[db]["*"]; ok {
				return true
			}
			if _, ok = This.ReplicateIgnoreDbLike[db]; ok {
				for k, _ := range This.ReplicateIgnoreDbLike[db] {
					reqTagAll, err := regexp.Compile(k)
					if err != nil {
						continue
					}
					if reqTagAll.FindString(table) != "" {
						This.addReplicateIgnoreDb0(db, table)
						return true
					}
				}
			}
		}
		return false
	}
	return true
}
