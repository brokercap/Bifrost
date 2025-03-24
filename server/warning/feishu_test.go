//go:build integration
// +build integration

package warning_test

import (
	"log"
	"testing"

	"github.com/brokercap/Bifrost/server/warning"
)

func TestFeishuSendWarning(t *testing.T) {
	obj := &warning.Feishu{}

	p := make(map[string]interface{}, 0)

	p["webhook"] = "https://open.feishu.cn/open-apis/bot/v2/hook/c8625af3-480a-4fce-8d2c-03b2436cfccf"

	err := obj.SendWarning(p, "test warning title", "it is test")
	if err != nil {
		t.Errorf(err.Error())
	} else {
		log.Println("success")
	}
}
