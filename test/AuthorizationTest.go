package main

import (
	"time"
	"net/http"
	"log"
	"io/ioutil"
	"crypto/tls"
)

func main(){
	//跳过证书验证
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	url := "https://127.0.0.1:21036/db/list?format=json"
	client := &http.Client{Timeout:10 * time.Second,Transport: tr,}
	req, err := http.NewRequest("GET",url, nil)
	if err != nil {
		log.Println(err)
		return
	}
	req.SetBasicAuth("Bifrost","Bifrost123")
	resp, err2 := client.Do(req)
	if err2 != nil{
		log.Println(err2)
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)

	log.Println("body:",string(body))
}