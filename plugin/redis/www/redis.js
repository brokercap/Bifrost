function doGetPluginParam(){
	var result = {data:{},status:false,msg:"error",batchSupport:true}
    var data = {};
	var Type = $("#Redis_Plugin_Contair select[name='type']").val();

    var DataType = $("#Redis_Plugin_Contair #Redis_DataType").val();
    var KeyConfig = $("#Redis_Plugin_Contair input[name='KeyConfig']").val();
	var ValueConfig = $("#Redis_Plugin_Contair #ValueConfig").val();
    if (KeyConfig==""){
		result.msg = "Key must be not empty!"
		return result
    }
    if (DataType == "string" && ValueConfig==""){
		result.msg = "DataType==string,ValueConfig muest be!"
        return result;
    }
	
	var Expir = $("#Redis_Plugin_Contair input[name='Expir']").val();

    if (Expir != "" && Expir != null && isNaN(Expir)){
		result.msg = "Expir must be int!"
        return result;
    }
    data["KeyConfig"] = KeyConfig;
    data["ValueConfig"] = ValueConfig;
    data["Expir"] = parseInt(Expir);
	data["DataType"] = DataType;
	data["Type"] = Type;
	result.data = data;
	result.msg = "success";
	result.status = true;
    return result;
}

function Redis_DataType_Change(){
	var dataType = $("#Redis_DataType").val();
	if(dataType == "string"){
		$("#Redis_ValueConfigContair").show();
	}else{
		$("#Redis_ValueConfigContair").hide();
	}
}