function doGetPluginParam(){
    var data = {};
	var result = {data:{},status:true,msg:"success",batchSupport:true};
    data["ContentType"]  = $("#Http_Plugin_Contair #Http_ContentType").val();
    data["Timeout"] = parseInt($("#Http_Plugin_Contair #Http_TimeOut").val());
    result.data = data;
	return result;
}

setPluginParamDefault("FilterQuery",false);