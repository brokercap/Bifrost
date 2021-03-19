function doGetPluginParam() {
    var result = {data: {}, status: false, msg: "error", batchSupport: true}
    var data = {};

    var Type = $("#Redis_Plugin_Container #Redis_Type").val();
    var DataType = $("#Redis_Plugin_Container #Redis_DataType").val();
    var KeyConfig = $("#Redis_Plugin_Container #Redis_KeyConfig").val();
    var ValueConfig = $("#Redis_Plugin_Container #Redis_ValueConfig").val();
    var HashKeyConfig = $("#Redis_Plugin_Container #Redis_HashKeyConfig").val();
    var SortConfig = $("#Redis_Plugin_Container #Redis_SortConfig").val();
    var Expired = $("#Redis_Plugin_Container #Redis_Expired").val();

    if (!KeyConfig){
    	result.msg = "Key could not be empty!"
    	return result
    }

    if (DataType === "custom" && !ValueConfig){
    	result.msg = "DataType==custom,Custom Value could not be empty!"
        return result;
    }else if (DataType === "custom"){
		data["ValConfig"] = ValueConfig;
	}

    // 不同的类型有不同参数
    switch (Type){
    	case 'set': {
    		try {
    			data["Expired"] = parseInt(Expired)
    		}catch (e) {
    			result.msg = 'Expired not belong to int'
    			return
    		}
    		break
    	}
    	case 'hash': {
    		if (!HashKeyConfig){
    			result.msg = 'Expired not belong to int'
    			return
    		}
    		data["HashKey"] = HashKeyConfig;
    		break
    	}
    	case 'zset': {
    		if (!HashKeyConfig){
    			result.msg = 'Expired not belong to int'
    			return
    		}
    		data["Sort"] = SortConfig;
    		break
    	}
    }

    data["KeyConfig"] = KeyConfig;
    data["DataType"] = DataType;
    data["Type"] = Type;

    result.data = data
    result.msg = "success"
    result.status = true

	console.log(result)
    return result;
}

function onRedisDataTypeChange(){
	if($("#Redis_DataType").val() === "custom")
		$("#Redis_ValueConfigContainer").show()
	else
		$("#Redis_ValueConfigContainer").hide()
}

function onRedisTypeChange(){
	var RedisHashKeyConfigContainer = $("#Redis_HashKeyConfigContainer")
	var RedisSortConfigContainer = $("#Redis_SortConfigContainer")
	var RedisExpiredContainer = $("#Redis_ExpiredContainer")

	RedisHashKeyConfigContainer.hide()
	RedisSortConfigContainer.hide()
	RedisExpiredContainer.hide()

	switch ($("#Redis_Type").val()){
		case 'string': {
			RedisExpiredContainer.show()
			break
		}
		case 'hash': {
			RedisHashKeyConfigContainer.show()
			break
		}
		case 'zset': {
			RedisSortConfigContainer.show()
			break
		}
	}
}
