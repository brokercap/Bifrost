function doGetPluginParam(){
	var result = {data:{},status:false,msg:"error",batchSupport:true}
    var data = {};
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