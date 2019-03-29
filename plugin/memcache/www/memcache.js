function doGetPluginParam(){
	var result = {data:{},status:false,msg:"error"}
    var data = {};
    var AddEventType = false;
	if ($("#Memcache_Plugin_Contair input[name='AddEventType']:checked").val() == "1"){
		AddEventType = true;
	}
	var AddSchemaName = false;
	if ($("#Memcache_Plugin_Contair input[name='AddSchemaName']:checked").val() == "1"){
		AddSchemaName = true;
	}
	var AddTableName = false;
    if ($("#Memcache_Plugin_Contair input[name='AddTableName']:checked").val() == "1"){
		AddTableName = true;
	}
	
    var DataType = $("#Memcache_Plugin_Contair #Memcache_DataType").val();
    var KeyConfig = $("#Memcache_Plugin_Contair input[name='KeyConfig']").val();
	var ValueConfig = $("#Memcache_Plugin_Contair #ValueConfig").val();
    if (KeyConfig==""){
		result.msg = "Key must be not empty!"
		return result
    }
    if (DataType == "string" && ValueConfig==""){
		result.msg = "DataType==string,ValueConfig muest be!"
        return result;
    }
	
	var Expir = $("#Memcache_Plugin_Contair input[name='Expir']").val();

    if (Expir != "" && Expir != null && isNaN(Expir)){
		result.msg = "Expir must be int!"
        return result;
    }

    data["AddSchemaName"] = AddSchemaName;
    data["AddTableName"] = AddTableName;
    data["AddEventType"] = AddEventType;
    data["KeyConfig"] = KeyConfig;
    data["ValueConfig"] = ValueConfig;
    data["Expir"] = parseInt(Expir);
	data["DataType"] = DataType;
	result.data = data;
	result.msg = "success";
	result.status = true;
    return result;
}

function Memcache_DataType_Change(){
	var dataType = $("#Memcache_DataType").val();
	if(dataType == "string"){
		$("#Memcache_ValueConfigContair").show();
	}else{
		$("#Memcache_ValueConfigContair").hide();
	}
}