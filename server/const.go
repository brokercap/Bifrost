package server

type StatusFlag string

const (
	DEFAULT  StatusFlag = ""
	STARTING StatusFlag = "starting"
	RUNNING  StatusFlag = "running"
	STOPPING StatusFlag = "stopping"
	STOPPED  StatusFlag = "stopped"
	CLOSING  StatusFlag = "closing"
	CLOSED   StatusFlag = "closed"
	KILLING  StatusFlag = "killing"
	KILLED   StatusFlag = "killed"
	DELING   StatusFlag = "deling"
	DELED    StatusFlag = "deled"
)
