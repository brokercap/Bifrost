function doGetPluginParam(){
	let result = {data:{},status:false,msg:"error",batchSupport:true}
    let data = {};

	let type = $("#Redis_Plugin_Container #Plugin_type").val();
    let keyConfig = $("#Redis_Plugin_Container #KeyConfig").val();
	let fieldKeyConfig = $("#Redis_FieldKeyConfigContainer #FieldKeyConfig").val();
    if (keyConfig === ""){
		result.msg = "Key must be not empty!"
		return result
    }
	if (fieldKeyConfig ==="" && type === "hash"){
		result.msg = "Hash FieldKey must be not empty!"
		return result
	}
	
	let Expir = $("#Redis_Plugin_Container input[name='Expir']").val();

    if (!Expir && isNaN(Expir)){
		result.msg = "expired must be int!"
        return result;
    }
    
    data["KeyConfig"] = keyConfig;
    data["FieldKeyConfig"] = fieldKeyConfig;
    data["Expir"] = parseInt(Expir);
	data["Type"] = type;
	result.batchSupport = true;
	result.data = data;
	result.msg = "success";
	result.status = true;
    return result;
}

function redisTypeChange(){
	let type = $("#Plugin_type").val();
	if(type === "hash"){
		$("#Redis_FieldKeyConfigContainer").show();
	}else{
		$("#Redis_FieldKeyConfigContainer").hide();
	}
}

redisTypeChange()