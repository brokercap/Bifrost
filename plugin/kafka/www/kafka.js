function doGetPluginParam(){
	var result = {data:{},status:false,msg:"error",batchSupport:true}
    var data = {};

	var Topic = $("#Kafka_Topic").val();
	var Key = $("#Kafka_Key").val();
	var BatchSize = $("#Kafka_BatchSize").val();
    var Timeout = $("#Kafka_Timeout").val();
    var RequiredAcks = $("#Kafka_RequiredAcks").val();
    var OtherObjectType = $("#Kafka_OtherObjectType").val();
	
    if (Topic == ""){
		result.msg = "Topic can't be empty"
        return result;
    }
	
    if (BatchSize == "" || BatchSize == null || isNaN(BatchSize) || BatchSize < 1 ){
		result.msg = "BatchSize must be uint!";
        return result;
    }
    if (Timeout == "" || Timeout == null || isNaN(Timeout)){
        result.msg = "Timeout must be int!";
        return result;
    }
	if ( RequiredAcks != "-1" && RequiredAcks != "0" && RequiredAcks != "1" ){
        result.msg = "RequiredAcks must be -1 | 0 | 1 !";
        return result;
	}

	data["Topic"] = Topic;
	data["Key"] = Key;
	data["BatchSize"] = parseInt(BatchSize);
    data["Timeout"] = parseInt(Timeout);
    data["RequiredAcks"] = parseInt(RequiredAcks);
    data["OtherObjectType"] = OtherObjectType;

	result.data = data;
	result.msg = "success";
	result.status = true;
    return result;
}

function initKafkaSupportedOtherOutputTypeList(){
    $.get(
        "/plugin/getSupportedOtherOutputTypeList",
        function (d, status) {
            if (status != "success") {
                return false;
            }
            var html = "";
            var defaultValue = null;
            for (var i in d) {
                var typeName = d[i].name;
                var value = d[i].value;
                if (defaultValue == null) {
                    defaultValue = value
                }
                html += "<option value=\"" + value + "\">" + typeName + "</option>";
            }
            $("#Kafka_OtherObjectType").html(html);
            if (defaultValue != null) {
                $("#Kafka_OtherObjectType").val(defaultValue);
            }
        }, 'json');
}

initKafkaSupportedOtherOutputTypeList();

setPluginParamDefault("FilterQuery",false);