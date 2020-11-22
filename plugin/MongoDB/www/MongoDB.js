
function doGetPluginParam(){
	var result = {data:{},status:false,msg:"error",batchSupport:true}
    var data = {};

	var SchemaName = $("#MongoDB_SchemaName").val();
    var TableName = $("#MongoDB_TableName").val();
    var PrimaryKey = $("#MongoDB_PrimaryKey").val();

    if (SchemaName == ""){
        result.msg = "SchemaName can't be empty!";
        return result;
    }

    if (TableName == ""){
        result.msg = "TableName can't be empty!";
        return result;
    }

	data["SchemaName"] = SchemaName;
	data["TableName"] = TableName;
	data["PrimaryKey"] = PrimaryKey;

	result.data = data;
	result.msg = "success";
	result.status = true;
    return result;
}
