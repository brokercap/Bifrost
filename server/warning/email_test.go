//go:build integration
// +build integration

package warning_test

import (
	"github.com/brokercap/Bifrost/server/warning"
	"log"
	"testing"
)

func TestSendWarning(t *testing.T) {
	obj := &warning.Email{}

	p := make(map[string]interface{}, 0)

	p["FROM"] = "111@qq.com"
	p["TO"] = "22222@126.com"
	p["Password"] = "sdfqkydtluocwuzcbdfr45"
	p["SmtpHost"] = "smtp.qq.com"
	p["SmtpPort"] = 25
	p["NickName"] = "test nick name"

	err := obj.SendWarning(p, "test warning title", "it is test")
	if err != nil {
		t.Errorf(err.Error())
	} else {
		log.Println("success")
	}
}
