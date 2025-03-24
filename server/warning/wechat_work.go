package warning

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

var wechatLock sync.RWMutex

type wechatAccessToken struct {
	AccessToken string
	ExpireTime  int64
}

var wechatAccessTokenMap map[string]wechatAccessToken

func init() {
	wechatAccessTokenMap = make(map[string]wechatAccessToken, 0)
	Register("WechatWork", &WechatWork{})
}

func getWechatAccessToken(corpid, corpsecret string) string {
	wechatLock.Lock()
	defer wechatLock.Unlock()
	key := corpid + "-" + corpsecret
	if _, ok := wechatAccessTokenMap[key]; !ok {
		updateWechatAccessToken(corpid, corpsecret)
	} else {
		if wechatAccessTokenMap[key].ExpireTime < time.Now().Unix() {
			updateWechatAccessToken(corpid, corpsecret)
		}
	}
	if _, ok := wechatAccessTokenMap[key]; !ok {
		return ""
	}
	return wechatAccessTokenMap[key].AccessToken
}

func delWechatAccessToken(corpid, corpsecret string) bool {
	wechatLock.Lock()
	defer wechatLock.Unlock()
	key := corpid + "-" + corpsecret
	delete(wechatAccessTokenMap, key)
	return true
}

func updateWechatAccessToken(corpid, corpsecret string) bool {
	url := "https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=" + corpid + "&corpsecret=" + corpsecret
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Println("updateWechatAccessToken err:", err)
		return false
	}
	resp, err2 := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err2 != nil {
		log.Println("updateWechatAccessToken err:", err2)
		return false
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("updateWechatAccessToken err:", err)
		return false
	}
	type result struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
		Errcode     int    `json:"errcode"`
		Errmsg      string `json:"errmsg"`
	}
	var data result
	err1 := json.Unmarshal(body, &data)
	if err1 != nil {
		log.Println("updateWechatAccessToken err:", err1, "data:", string(body))
		return false
	}
	if data.AccessToken != "" {
		wechatAccessTokenMap[corpid+"-"+corpsecret] = wechatAccessToken{
			AccessToken: data.AccessToken,
			ExpireTime:  time.Now().Unix() + data.ExpiresIn - 60,
		}
	} else {
		log.Println("updateWechatAccessToken err data:", string(body))
		return false
	}
	return true
}

func sendToWeChatMsg(corpid, corpsecret string, dataBody string) error {
	AccessToken := getWechatAccessToken(corpid, corpsecret)
	if AccessToken == "" {
		return fmt.Errorf("AccessToken is empty")
	}
	url := "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=" + AccessToken
	client := &http.Client{Timeout: 10 * time.Second}
	payload := strings.NewReader(dataBody)
	req, err := http.NewRequest("POST", url, payload)
	if err != nil {
		log.Println("sendToWeChatMsg err:", err, "data:", dataBody)
		return err
	}
	res, err := client.Do(req)
	if res != nil {
		defer res.Body.Close()
	}
	if err != nil {
		log.Println("sendToWeChatMsg err:", err, "data:", dataBody)
		return err
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Println("sendToWeChatMsg err:", err, "data:", dataBody)
		return err
	}

	type result struct {
		errcode      int    `json:"errcode"`
		errmsg       string `json:"errmsg"`
		invaliduser  string `json:"invaliduser"`
		invalidparty string `json:"invalidparty"`
		invalidtag   string `json:"invalidtag"`
	}

	var d result
	err2 := json.Unmarshal(body, &d)
	if err2 != nil {
		log.Println("sendToWeChatMsg err:", err2, "body:", string(body))
		return err
	}
	if d.errcode == 40014 {
		delWechatAccessToken(corpid, corpsecret)
		return fmt.Errorf(d.errmsg)
	}
	if d.errcode != 0 {
		log.Println("sendToWeChatMsg result:", d, "data:", dataBody)
	}
	return nil
}

type WechatWork struct {
	p WeChatWorkParam
}

type WeChatWorkParam struct {
	ToUser     string `json:"touser"`
	ToParty    string `json:"toparty"`
	ToTag      string `json:"totag"`
	AgentId    int    `json:"agentid"`
	CorpID     string `json:"corpid"`
	CorpSecret string `json:"corpsecret"`
}

func (This *WechatWork) paramTansfer(p map[string]interface{}) error {
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

func (This *WechatWork) SendWarning(p map[string]interface{}, title string, Body string) error {
	err1 := This.paramTansfer(p)
	if err1 != nil {
		return err1
	}
	if This.p.CorpID == "" || This.p.CorpSecret == "" || This.p.AgentId <= 0 {
		return fmt.Errorf("corpid or corpsecret or agentid param error")
	}

	type msgtext struct {
		Content string `json:"content"`
	}
	type msg struct {
		Touser  string  `json:"touser"`
		Toparty string  `json:"toparty"`
		Totag   string  `json:"totag"`
		Agentid int     `json:"agentid"`
		Msgtype string  `json:"msgtype"`
		Text    msgtext `json:"text"`
		Safe    int
	}

	data := msg{
		Touser:  This.p.ToUser,
		Toparty: This.p.ToParty,
		Totag:   This.p.ToTag,
		Agentid: This.p.AgentId,
		Msgtype: "text",
		Text:    msgtext{Content: Body},
		Safe:    0,
	}

	b, err := json.Marshal(data)
	if err != nil {
		log.Println("sendToWeChatMsg json.Marshal err:", err)
		return err
	}
	return sendToWeChatMsg(This.p.CorpID, This.p.CorpSecret, string(b))
}
