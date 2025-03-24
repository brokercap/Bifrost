package controller

import (
	"encoding/json"
	"github.com/brokercap/Bifrost/server/user"
	"io/ioutil"
)

type RefuseIpController struct {
	CommonController
}

type RefuseIpParam struct {
	Ip string
}

func (c *RefuseIpController) getParam() *RefuseIpParam {
	body, err := ioutil.ReadAll(c.Ctx.Request.Body)
	if err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	var data RefuseIpParam
	if err = json.Unmarshal(body, &data); err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	return &data
}

func (c *RefuseIpController) Index() {
	RefuseIpMap := user.GetRefuseIpMap()
	c.SetData("RefuseIpMap", RefuseIpMap)
	c.SetTitle("Refuse Ip Manager")
	c.AddAdminTemplate("refuse.ip.list.html", "header.html", "footer.html")
}

func (c *RefuseIpController) Del() {
	param := c.getParam()
	result := ResultDataStruct{Status: 1, Msg: "success", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	user.DelRefuseIp(param.Ip)
}

func (c *RefuseIpController) List() {
	c.SetJsonData(user.GetRefuseIpMap())
	c.StopServeJSON()
}
