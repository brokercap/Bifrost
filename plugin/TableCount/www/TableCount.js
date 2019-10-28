function doGetPluginParam() {
    var result = {data:{},status:false,msg:"error"}
    var data = {};
    if (dbname == undefined || dbname == ""){
        result.msg = "dbname error";
        return result
	}
    data["DbName"] = dbname;
    result.data = data;
    result.msg = "success";
    result.status = true;
    return result;
}