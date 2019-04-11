package warning_test

import (
	"testing"
	"github.com/jc3wish/Bifrost/server/warning"
	"log"
)

func TestSendWarning(t *testing.T){
	obj := &warning.Email{}

	p := make(map[string]interface{},0)

	p["FROM"] = "237633006@qq.com"
	p["TO"] = []string{"jc3wish@126.com"}
	p["Password"] = "qkydtluocwuzcbdf"
	p["SmtpHost"] = "smtp.qq.com"
	p["SmtpPort"] = 25
	p["NickName"] = "test nick name"

	err := obj.SendWarning(p,"test warning")
	if err != nil{
		t.Errorf(err.Error())
	}else{
		log.Println("success")
	}
}