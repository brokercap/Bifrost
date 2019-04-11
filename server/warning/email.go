package warning

import (
	"net/smtp"
	"strings"
	"encoding/json"
	"strconv"
)

func init()  {
	Register("Email",&Email{})
}

type Email struct {
	p EmailParam
}

type EmailParam struct {
	From string
	To []string
	Password string
	SmtpHost string
	SmtpPort int
	NickName string
}

func (This *Email) paramTansfer(p map[string]interface{}) error{
	s,err := json.Marshal(p)
	if err != nil{
		return err
	}
	err2 := json.Unmarshal(s,&This.p)
	if err2 != nil{
		return err2
	}
	return nil
}

func (This *Email) SendWarning(p map[string]interface{},title,Body string) error {
	err1 := This.paramTansfer(p)
	if err1 != nil{
		return err1
	}
	auth := smtp.PlainAuth("", This.p.From, This.p.Password, This.p.SmtpHost)
	subject := "Bifrost warning mail"
	content_type := "Content-Type: text/plain; charset=UTF-8"
	msg := []byte("To: " + strings.Join(This.p.To, ",") + "\r\nFrom: " + This.p.NickName +
		"<" + This.p.From + ">\r\nSubject: " + subject + "\r\n" + content_type + "\r\n\r\n" + Body)
	err := smtp.SendMail(This.p.SmtpHost+":"+strconv.Itoa(This.p.SmtpPort), auth, This.p.From, This.p.To, msg)
	if err != nil {
		return err
	}
	return  nil
}