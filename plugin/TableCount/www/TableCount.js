function doGetPluginParam(p) {
    var result = {data:{},status:false,msg:"error",batchSupport:true}
    var data = {};
    if (p.DbName == undefined || p.DbName == ""){
        result.msg = "DbName error";
        return result
	}
    data["DbName"] = p.DbName;
    result.data = data;
    result.msg = "success";
    result.status = true;
    return result;
}

setPluginParamDefault("FilterQuery",false);