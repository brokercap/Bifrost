package server

import (
	"encoding/json"
	"os"
	"testing"
)

func TestRecoveryJSON(t *testing.T) {
	var str string = `{"Version":"v1.4.2-release","ToServer":{"bk27":{"PluginName":"mysql","PluginVersion":"v1.3.2","ConnUri":"xieyuhua:xieyuhua@tcp(192.168.9.22:3306)/xieyuhua","Notes":"27","LastID":0,"CurrentConn":0,"MaxConn":50,"AvailableConn":0}},"DbInfo":{"bk26":{"Name":"bk26","ConnectUri":"xieyuhua:xieyuhua@tcp(192.168.9.26:3306)/duanxin","ConnStatus":"close","ConnErr":"","ChannelMap":{"1":{"Name":"default","MaxThreadNum":1,"CurrentThreadNum":0,"Status":"close"}},"LastChannelID":1,"TableMap":{"duanxin_-szy_user_3":{"Name":"szy_user_3","ChannelKey":1,"LastToServerID":1,"ToServerList":[{"ToServerID":1,"PluginName":"mysql","MustBeSuccess":true,"FilterQuery":true,"FilterUpdate":true,"FieldList":[],"ToServerKey":"bk27","BinlogFileNum":0,"BinlogPosition":0,"PluginParam":{"BatchSize":500,"Field":[{"FromMysqlField":"user_id","ToField":"user_id"},{"FromMysqlField":"user_name","ToField":"user_name"},{"FromMysqlField":"mobile","ToField":"mobile"},{"FromMysqlField":"email","ToField":"email"},{"FromMysqlField":"is_seller","ToField":"is_seller"},{"FromMysqlField":"shop_id","ToField":"shop_id"},{"FromMysqlField":"store_id","ToField":"store_id"},{"FromMysqlField":"multi_store_id","ToField":"multi_store_id"},{"FromMysqlField":"init_password","ToField":"init_password"},{"FromMysqlField":"comstore_id","ToField":"comstore_id"},{"FromMysqlField":"password","ToField":"password"},{"FromMysqlField":"password_reset_token","ToField":"password_reset_token"},{"FromMysqlField":"salt","ToField":"salt"},{"FromMysqlField":"nickname","ToField":"nickname"},{"FromMysqlField":"sex","ToField":"sex"},{"FromMysqlField":"birthday","ToField":"birthday"},{"FromMysqlField":"address_now","ToField":"address_now"},{"FromMysqlField":"detail_address","ToField":"detail_address"},{"FromMysqlField":"headimg","ToField":"headimg"},{"FromMysqlField":"faceimg1","ToField":"faceimg1"},{"FromMysqlField":"faceimg2","ToField":"faceimg2"},{"FromMysqlField":"user_money","ToField":"user_money"},{"FromMysqlField":"user_money_limit","ToField":"user_money_limit"},{"FromMysqlField":"frozen_money","ToField":"frozen_money"},{"FromMysqlField":"pay_point","ToField":"pay_point"},{"FromMysqlField":"frozen_point","ToField":"frozen_point"},{"FromMysqlField":"rank_point","ToField":"rank_point"},{"FromMysqlField":"address_id","ToField":"address_id"},{"FromMysqlField":"rank_id","ToField":"rank_id"},{"FromMysqlField":"rank_start_time","ToField":"rank_start_time"},{"FromMysqlField":"rank_end_time","ToField":"rank_end_time"},{"FromMysqlField":"mobile_validated","ToField":"mobile_validated"},{"FromMysqlField":"email_validated","ToField":"email_validated"},{"FromMysqlField":"reg_time","ToField":"reg_time"},{"FromMysqlField":"reg_ip","ToField":"reg_ip"},{"FromMysqlField":"last_time","ToField":"last_time"},{"FromMysqlField":"last_ip","ToField":"last_ip"},{"FromMysqlField":"visit_count","ToField":"visit_count"},{"FromMysqlField":"mobile_supplier","ToField":"mobile_supplier"},{"FromMysqlField":"mobile_province","ToField":"mobile_province"},{"FromMysqlField":"mobile_city","ToField":"mobile_city"},{"FromMysqlField":"reg_from","ToField":"reg_from"},{"FromMysqlField":"surplus_password","ToField":"surplus_password"},{"FromMysqlField":"status","ToField":"status"},{"FromMysqlField":"auth_key","ToField":"auth_key"},{"FromMysqlField":"type","ToField":"type"},{"FromMysqlField":"is_real","ToField":"is_real"},{"FromMysqlField":"shopping_status","ToField":"shopping_status"},{"FromMysqlField":"comment_status","ToField":"comment_status"},{"FromMysqlField":"role_id","ToField":"role_id"},{"FromMysqlField":"auth_codes","ToField":"auth_codes"},{"FromMysqlField":"company_name","ToField":"company_name"},{"FromMysqlField":"company_region_code","ToField":"company_region_code"},{"FromMysqlField":"company_address","ToField":"company_address"},{"FromMysqlField":"purpose_type","ToField":"purpose_type"},{"FromMysqlField":"referral_mobile","ToField":"referral_mobile"},{"FromMysqlField":"employees","ToField":"employees"},{"FromMysqlField":"industry","ToField":"industry"},{"FromMysqlField":"nature","ToField":"nature"},{"FromMysqlField":"contact_name","ToField":"contact_name"},{"FromMysqlField":"department","ToField":"department"},{"FromMysqlField":"company_tel","ToField":"company_tel"},{"FromMysqlField":"qq_key","ToField":"qq_key"},{"FromMysqlField":"weibo_key","ToField":"weibo_key"},{"FromMysqlField":"weixin_key","ToField":"weixin_key"},{"FromMysqlField":"user_remark","ToField":"user_remark"},{"FromMysqlField":"invite_code","ToField":"invite_code"},{"FromMysqlField":"parent_id","ToField":"parent_id"},{"FromMysqlField":"is_recommend","ToField":"is_recommend"},{"FromMysqlField":"customs_money","ToField":"customs_money"},{"FromMysqlField":"audit_status","ToField":"audit_status"},{"FromMysqlField":"enterprise_type_id","ToField":"enterprise_type_id"},{"FromMysqlField":"contacts","ToField":"contacts"},{"FromMysqlField":"corporation_name","ToField":"corporation_name"},{"FromMysqlField":"business_address","ToField":"business_address"},{"FromMysqlField":"offline_id","ToField":"offline_id"},{"FromMysqlField":"submit_time","ToField":"submit_time"},{"FromMysqlField":"error_reason","ToField":"error_reason"},{"FromMysqlField":"is_first_market","ToField":"is_first_market"},{"FromMysqlField":"hd_region_codes","ToField":"hd_region_codes"},{"FromMysqlField":"hd_group_id","ToField":"hd_group_id"},{"FromMysqlField":"business_license_id","ToField":"business_license_id"},{"FromMysqlField":"business_license_name","ToField":"business_license_name"},{"FromMysqlField":"business_license_validity","ToField":"business_license_validity"},{"FromMysqlField":"business_qualification","ToField":"business_qualification"},{"FromMysqlField":"is_sync","ToField":"is_sync"}],"NullTransferDefault":false,"PriKey":[{"FromMysqlField":"user_id","ToField":"user_id"}],"Schema":"xieyuhua","SyncMode":"Normal","Table":"szy_user"},"Status":"","Error":"","ErrorWaitDeal":0,"ErrorWaitData":null,"LastBinlogFileNum":0,"LastBinlogPosition":0,"QueueMsgCount":0,"FileQueueStatus":false,"Notes":"","ThreadCount":0,"FileQueueUsableCount":0,"FileQueueUsableCountStartTime":0,"CosumerIdInrc":0}]}},"BinlogDumpFileName":"mysql-bin.000020","BinlogDumpPosition":16754506,"MaxBinlogDumpFileName":"","MaxinlogDumpPosition":0,"ReplicateDoDb":{"duanxin":1},"ServerId":24,"AddTime":1599481218}},"User":[{"Name":"Bifrost","Password":"xieyuhua","Group":"administrator","AddTime":1599271201,"UpdateTime":1599283956},{"Name":"BifrostMonitor","Password":"Bifrost123","Group":"monitor","AddTime":1599271201,"UpdateTime":1599271201}],"Warning":{"-bifrost-bifrost_warning_config_1":{"Type":"WechatWork","Param":{"agentid":1000025,"corpid":"wwae773aaa198a444c","corpsecret":"0fYkdkrzK-fA5O9r9KBFctCijL6grRJbmJP5v6D6E14","toparty":"","totag":"","touser":"XieYuHua"}},"-bifrost-bifrost_warning_config_2":{"Type":"Email","Param":{"From":"1510120461@qq.com","NickName":"","Password":"usbqjomsnoqvhhjj","SmtpHost":"smtp.qq.com","SmtpPort":25,"To":"1510120461@qq.com"}}}}`
	var data map[string]dbSaveInfo

	var recoveryData recovery
	errors := json.Unmarshal([]byte(str), &recoveryData)
	if errors != nil {
		t.Fatal("recovery error:", errors.Error())
		return
	}
	errors = json.Unmarshal(*recoveryData.DbInfo, &data)
	if errors != nil {
		t.Fatal("recorery db content errors;", errors)
		os.Exit(1)
		return
	}
	if len(data["bk26"].TableMap) == 0 {
		t.Fatal("TableMap is empty")
	}
	if !recoveryData.StartTime.IsZero() {
		t.Fatal("StartTime is not zero")
	}
	t.Log(data)
}

func TestRecoveryJSON_StartTime(t *testing.T) {
	var str string = `{"Version":"v1.4.2-release","StartTime":"2024-04-03T15:04:05Z"}`
	var recoveryData recovery
	errors := json.Unmarshal([]byte(str), &recoveryData)
	if errors != nil {
		t.Fatal("recovery error:", errors.Error())
		return
	}
	if recoveryData.StartTime.Format("2006-01-02") != "2024-04-03" {
		t.Fatal("recovery StartTime != 2024-04-03")
		return
	}
}
