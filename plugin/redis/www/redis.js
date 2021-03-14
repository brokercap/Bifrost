function doGetPluginParam(){
	console.log('test')
	var result = {data:{},status:false,msg:"error",batchSupport:true}
    var data = {};

	var Type = $("#Redis_Plugin_Container select[name='type']").val();
    var DataType = $("#Redis_Plugin_Container #Redis_DataType").val();
    var KeyConfig = $("#Redis_Plugin_Container input[name='KeyConfig']").val();
	var ValueConfig = $("#Redis_Plugin_Container #ValueConfig").val();
	var HashKeyConfig = $("#Redis_HashKeyConfigContainer #HashKeyConfig").val();
	var SortConfig = $("#Redis_SortConfigContainer #SortConfig").val();
	var Expired = $("#Redis_Plugin_Container input[name='Expired']").val();

    if (!KeyConfig){
		result.msg = "Key could not be empty!"
		return result
    }
    if (DataType === "string" && !ValueConfig){
		result.msg = "DataType==custom,Custom Value could not be empty!"
        return result;
    }

    // 不同的类型有不同参数
    switch (Type){
		case 'string': {
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
	data["ValueConfig"] = ValueConfig;
	data["DataType"] = DataType;
	data["Type"] = Type;

	result.data = data;
	result.msg = "success";
	result.status = true;

	console.log(result)
    return result;
}

function Redis_DataType_Change(){
	var dataType = $("#Redis_DataType").val();
	if(dataType === "custom") $("#Redis_ValueConfigContainer").show() else $("#Redis_ValueConfigContainer").hide()
}

function Redis_Type_Change(){
	var type = $("#Redis_Type").val();

	var RedisHashKeyConfigContainer = $("#Redis_HashKeyConfigContainer")
	var RedisSortConfigContainer = $("#Redis_SortConfigContainer")
	var RedisExpiredContainer = $("#Redis_ExpiredContainer")

	RedisHashKeyConfigContainer.hide()
	RedisSortConfigContainer.hide()
	RedisExpiredContainer.hide()

	switch (type){
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
