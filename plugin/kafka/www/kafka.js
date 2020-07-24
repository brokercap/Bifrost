function doGetPluginParam(){
	var result = {data:{},status:false,msg:"error",batchSupport:true}
    var data = {};

	var Topic = $("#Kafka_Topic").val();
	var Key = $("#Kafka_Key").val();
	var BatchSize = $("#Kafka_BatchSize").val();
	
    if (Topic == ""){
		result.msg = "Topic can't be empty"
        return result;
    }
	
    if (BatchSize != "" && BatchSize != null && isNaN(BatchSize)){
		result.msg = "BatchSize must be int!"
        return result;
    }

	data["Topic"] = Topic;
	data["Key"] = Key;
	data["BatchSize"] = parseInt(BatchSize);

	result.data = data;
	result.msg = "success";
	result.status = true;
    return result;
}