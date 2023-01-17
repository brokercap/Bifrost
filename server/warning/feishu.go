package warning

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
)

type Feishu struct {
	p FeishuParam
}

type FeishuParam struct {
	Webhook string `json:"webhook"`
}

type PostData struct {
	MsgType string `json:"msg_type"`
	Content struct {
		Text string `json:"text"`
	} `json:"content"`
}

func init() {
	Register("Feishu", &Feishu{})
}

func (This *Feishu) paramTansfer(p map[string]interface{}) error {
	s, err := json.Marshal(p)
	if err != nil {
		return err
	}
	err2 := json.Unmarshal(s, &This.p)
	if err2 != nil {
		return err2
	}
	return nil
}

func (This *Feishu) SendWarning(p map[string]interface{}, title string, Body string) error {
	err1 := This.paramTansfer(p)
	if err1 != nil {
		return err1
	}

	data := PostData{}
	data.MsgType = "text"
	data.Content.Text = Body

	b, err := json.Marshal(data)
	if err != nil {
		log.Println("sendToWeChatMsg json.Marshal err:", err)
		return err
	}
	return sendFeishuMsg(This.p.Webhook, string(b))
}

func sendFeishuMsg(url string, json_data string) error {
	resp, err := http.Post(url, "application/json", strings.NewReader(json_data))
	if err != nil {
		return err
	}
	var res map[string]interface{}

	json.NewDecoder(resp.Body).Decode(&res)
	return nil
}
