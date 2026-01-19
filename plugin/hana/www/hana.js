function doGetPluginParam(){
    var result = {data:{},status:false,msg:"error",batchSupport:true}
    var data = {};
    
    var schema = $("#hana_schema").val();
    var table = $("#hana_table").val();
    var batchSize = $("#hana_BatchSize").val();

    if (batchSize != "" && batchSize != null && isNaN(batchSize)){
        result.msg = "BatchSize must be int!"
        return result;
    }

    data["Schema"] = schema;
    data["Table"] = table;
    data["BatchSize"] = parseInt(batchSize);

    result.data = data;
    result.msg = "success";
    result.status = true;
    return result;
}
