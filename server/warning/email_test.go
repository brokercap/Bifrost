package warning_test

import (
	"github.com/brokercap/Bifrost/server/warning"
	"log"
	"strings"
	"testing"
)

func TestUserSplit(t *testing.T) {
	p := make(map[string]string, 0)
	p["TO"] = "jc3wish@126.com;"
	tmp := strings.Split(p["TO"], ";")
	log.Println("len:", len(tmp))
	log.Println(tmp[len(tmp)-1])
	tmp = tmp[:len(tmp)-1]
	log.Println("tmp", tmp)
}

func TestSendWarning(t *testing.T) {
	obj := &warning.Email{}

	p := make(map[string]interface{}, 0)

	p["FROM"] = "237633006@qq.com"
	p["TO"] = "jc3wish@126.com"
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
