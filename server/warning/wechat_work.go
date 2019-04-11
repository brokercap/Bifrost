package warning

import (
	"strings"
	"encoding/json"
	"net/http"
	"io/ioutil"
	"log"
	"time"
	"fmt"
)

type wechatAccessToken struct {
	AccessToken string
	ExpireTime int64
}
var wechatAccessTokenMap map[string]wechatAccessToken

func init()  {
	wechatAccessTokenMap = make(map[string]wechatAccessToken,0)
	Register("WeChatWork",&Email{})
}

func getWechatAccessToken(corpid,corpsecret string) string{
	key := corpid+"-"+corpsecret
	if _,ok := wechatAccessTokenMap[key];!ok{
		updateWechatAccessToken(corpid,corpsecret)
	}else{
		if wechatAccessTokenMap[key].ExpireTime < time.Now().Unix(){
			updateWechatAccessToken(corpid,corpsecret)
		}
	}
	if _,ok := wechatAccessTokenMap[key];!ok{
		return ""
	}
	return wechatAccessTokenMap[key].AccessToken
}

func updateWechatAccessToken(corpid,corpsecret string) bool{
	url := "https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid="+corpid+"&corpsecret="+corpsecret
	client := &http.Client{Timeout:10 * time.Second}
	req, err := http.NewRequest("GET", url,nil)
	if err != nil {
		log.Println("updateWechatAccessToken err:",err)
		return false
	}
	resp, err2 := client.Do(req)
	if resp != nil{
		resp.Body.Close()
	}
	if err2 != nil {
		log.Println("updateWechatAccessToken err:",err2)
		return false
	}
	if resp.StatusCode >= 200 || resp.StatusCode<300{
		log.Println("updateWechatAccessToken http code:",resp.StatusCode)
		return false
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil{
		log.Println("updateWechatAccessToken err:",err)
		return false
	}
	type result struct {
		AccessToken string 	`json:"access_token"`
		ExpiresIn 	int64 	`json:"expires_in"`
		Errcode 	int 	`json:"errcode"`
		Errmsg 		string 	`json:"errmsg"`
	}
	var data result
	err1 := json.Unmarshal(body,&data)
	if err1 != nil{
		log.Println("updateWechatAccessToken err:",err1,"data:",string(body))
		return false
	}
	if data.AccessToken != "" {
		l.Lock()
		wechatAccessTokenMap[corpid+"-"+corpsecret] = wechatAccessToken{
			AccessToken: data.AccessToken,
			ExpireTime:  time.Now().Unix() + data.ExpiresIn - 60,
		}
		l.Unlock()
	}else{
		log.Println("updateWechatAccessToken err data:",string(body))
		return false
	}
	return true
}

func sendToWeChatMsg(AccessToken string,data interface{}) error{
	url := "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token="+AccessToken
	b,_ := json.Marshal(data)
	client := &http.Client{Timeout:10 * time.Second}
	payload := strings.NewReader(string(b))
	req, err := http.NewRequest("POST", url, payload)
	if err != nil{
		log.Println("updateWechatAccessToken err:",err,"data:",string(b))
		return err
	}
	res, err := client.Do(req)
	if res != nil{
		defer res.Body.Close()
	}
	if err != nil{
		log.Println("updateWechatAccessToken err:",err,"data:",string(b))
		return err
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil{
		log.Println("updateWechatAccessToken err:",err,"data:",string(b))
		return err
	}
	type result struct {
		errcode 		int `json:"errcode"`
		errmsg 			string `json:"errmsg"`
		invaliduser		string `json:"invaliduser"`
		invalidparty 	string `json:"invalidparty"`
		invalidtag 		string `json:"invalidtag"`
	}

	var d result
	err2 := json.Unmarshal(body,&d)
	if err2 != nil{
		log.Println("updateWechatAccessToken err:",err2,"body:",string(body))
		return err
	}
	return nil
}

type WeChatWork struct {
	p WeChatWorkParam
}

type WeChatWorkParam struct {
	touser 		string `json:"touser"`
	toparty 	string `json:"toparty"`
	totag 		string `json:"totag"`
	agentid 	int `json:"agentid"`
	corpid 		string `json:"corpid"`
	corpsecret 	string `json:"corpsecret"`
}

func (This *WeChatWork) paramTansfer(p map[string]interface{}) error{
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

func (This *WeChatWork) SendWarning(p map[string]interface{},title string,Body string) error {
	err1 := This.paramTansfer(p)
	if err1 != nil{
		return err1
	}

	if This.p.corpid == "" || This.p.corpsecret == "" || This.p.agentid <= 0{
		return fmt.Errorf("corpid or corpsecret or agentid param error")
	}

	type msgtext struct {
		content string `json:"content"`
	}
	type msg struct {
		touser 	string `json:"touser"`
		toparty string `json:"toparty"`
		totag 	string `json:"totag"`
		agentid int `json:"agentid"`
		msgtype string `json:"msgtype"`
		text 	msgtext `json:"text"`
		safe 	int
	}

	AccessToken := getWechatAccessToken(This.p.corpid,This.p.corpsecret)
	if AccessToken == ""{
		return fmt.Errorf("AccessToken is empty")
	}

	data := msg{
		touser:This.p.touser,
		toparty:This.p.toparty,
		totag:This.p.totag,
		agentid:This.p.agentid,
		msgtype:"text",
		text:msgtext{content:Body},
		safe:0,
	}
	return sendToWeChatMsg(AccessToken,data)
}