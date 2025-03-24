//go:build integration
// +build integration

package warning_test

import (
	"github.com/brokercap/Bifrost/server/warning"
	"log"
	"testing"
)

func TestWechatWorkSendWarning(t *testing.T) {
	obj := &warning.WechatWork{}

	p := make(map[string]interface{}, 0)

	p["corpid"] = "wwad9500c1541efa7c"
	p["corpsecret"] = "Lm7JVNi3bLXP7yffJVzUWxz0QAueiDGCBSOODMbTpnk"
	p["touser"] = "@all"
	p["toparty"] = ""
	p["totag"] = ""
	p["agentid"] = 1000000

	err := obj.SendWarning(p, "test warning title", "it is test")
	if err != nil {
		t.Errorf(err.Error())
	} else {
		log.Println("success")
	}
}
