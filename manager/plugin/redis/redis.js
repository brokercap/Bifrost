function doGetParam(){
	var result = {data:{},status:false,msg:"error"}
    var data = {};
	var Type = $("#Redis_Plugin_Contair input[name='type']:checked").val();
    var AddEventType = false;
	if ($("#Redis_Plugin_Contair input[name='AddEventType']:checked").val() == "1"){
		AddEventType = true;
	}
	var AddSchemaName = false;
	if ($("#Redis_Plugin_Contair input[name='AddSchemaName']:checked").val() == "1"){
		AddSchemaName = true;
	}
	var AddTableName = false;
    if ($("#Redis_Plugin_Contair input[name='AddTableName']:checked").val()){
		AddTableName = true;
	}
	
    var DataType = $("#Redis_Plugin_Contair #Redis_DataType").val();
	var KeyConfig = $("#Redis_Plugin_Contair input[name='KeyConfig']").val();
    var KeyConfig = $("#Redis_Plugin_Contair input[name='KeyConfig']").val();
	var ValueConfig = $("#Redis_Plugin_Contair input[name='ValueConfig']").val();
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

    data["AddSchemaName"] = AddSchemaName;
    data["AddTableName"] = AddTableName;
    data["AddEventType"] = AddEventType;
    data["KeyConfig"] = KeyConfig;
    data["ValueConfig"] = ValueConfig;
    data["Expir"] = Expir;
	data["DataType"] = DataType;
	data["Type"] = Type;
	result.data = data;
	result.msg = "success";
	result.status = true;
    return data;
}

function Redis_DataType_Change(){
	var dataType = $("#Redis_DataType").val();
	if(dataType == "string"){
		$("#Redis_ValueConfigContair").show();
		$("#Redis_KeyConfigContair").hide();
	}else{
		$("#Redis_ValueConfigContair").hide();
		$("#Redis_KeyConfigContair").show();	
	}
}