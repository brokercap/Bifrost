package driver

type StatusFlag string

const (
	RUNNING  StatusFlag = "running"
	STARTING StatusFlag = "starting"
	CLOSED   StatusFlag = "closed"
	CLOSING  StatusFlag = "closing"
	STOPPED  StatusFlag = "stopped"
	STOPPING StatusFlag = "stopping"
)

type SupportType int8

const (
	SupportFull            SupportType = 1
	SupportIncre           SupportType = 2
	SupportNeedMinPosition SupportType = 3
)
