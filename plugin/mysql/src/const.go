package src

type EventType int8

const (
	INSERT         EventType = 0
	UPDATE         EventType = 1
	DELETE         EventType = 2
	REPLACE_INSERT EventType = 3
	SQLTYPE        EventType = 4
)

type SyncMode string

const (
	SYNCMODE_NORMAL       SyncMode = "Normal"
	SYNCMODE_LOG_UPDATE   SyncMode = "LogUpdate"
	SYNCMODE_LOG_APPEND   SyncMode = "LogAppend"
	SYNCMODE_NO_SYNC_DATA SyncMode = "NoSyncData"
)

const BifrostAutoInrcFieldName = "bifrost_auto_inrc_id"

const OutputName = "mysql"
