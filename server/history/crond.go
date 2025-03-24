package history

import (
	"github.com/robfig/cron/v3"
)

var crodObj *cron.Cron

func init() {
	crodObj = cron.New()
	crodObj.Start()
}
