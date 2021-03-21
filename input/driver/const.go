package driver

type StatusFlag string

const (
	RUNNING StatusFlag = "running"
	STARTING StatusFlag = "starting"
	CLOSED StatusFlag = "closed"
	CLOSING StatusFlag = "closing"
	STOPPED StatusFlag = "stopped"
	STOPPING StatusFlag = "stopping"
)