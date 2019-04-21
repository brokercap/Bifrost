function doGetPluginParam(){
	var result = {data:{},status:true,msg:"success"}
	return result;
}

function showClickHouseCreateSQL() {
    var tableName = $("#tableToServerListContair").attr("table_name");
    var sql = getClickHouseTableCreateSQL(tableName);

    $("#showClickHouseCreateSQL .modal-title").html("ClickHouse CreateSQL For Table </br>"+tableName);
    $("#showClickHouseCreateSQL .modal-body").text(sql);
    $("#showClickHouseCreateSQL").modal('show');
}

function getClickHouseTableCreateSQL(tableName) {
	if($("#TableFieldsContair .fieldsname input").length == 0){
		alert("请先选择 MYSQL 表");
		return;
	}

	var ddlSql = "";
	var index = "";
    $.each($("#TableFieldsContair .fieldsname input"),
		function () {
			var jsonString = $(this).attr("data");
            var data = getTableFieldType($(this).val());
            var getDDL = function (Type) {
                if(data.COLUMN_TYPE.indexOf("unsigned") >0){
                    Type = "U"+Type;
                }
                if(ddlSql == ""){
                    ddlSql = data.COLUMN_NAME+" "+Type;
                }else{
                    ddlSql +=","+data.COLUMN_NAME+" "+Type;
                }
                if(data.COLUMN_KEY == ""){
                    return
                }
                if (data.COLUMN_KEY != "PRI" && data.IS_NULLABLE != "NO" ){
                    return
                }
                if(index == ""){
                    index = data.COLUMN_NAME;
                }else{
                    index += ","+data.COLUMN_NAME;
                }
            }
            switch(data.DATA_TYPE){
                case "tinyint":
                    return getDDL("Int8");
                    break;
                case "smallint":
                    return getDDL("Int16");
                    break;
                case "mediumint":
                    return getDDL("Int32");
                    break;
                case "int":
                    return getDDL("Int32");
                    break;
                case "bigint":
                    return getDDL("Int64");
                    break;
                case "char":
                case "varchar":
                case "text":
                case "blob":
                case "mediumblob":
                case "longblob":
                case "tinyblob":
                case "mediumtext":
                case "longtext":
                case "tinytext":
                case "enum":
                case "set":
                    return getDDL("String");
                    break;
                case "float":
                case "double":
                    return getDDL("Float64");
                    break;
                case "decimal":
                    return getDDL("Decimal");
                    break;
                case "time":
                    return getDDL("String");
                    break;
                case "date":
                    return getDDL("Date");
                    break;
                case "datetime":
                case "timestamp":
                    return getDDL("DateTime");
                    break;
                case "year":
                    return getDDL("Int16");
                    break;
                case "year":
                    return getDDL("Int16");
                    break;
                case "bit":
                    return getDDL("Int");
                    break
                default:
                    return getDDL("String");
                    break;
            }
        }
	);


    var SQL = "CREATE TABLE "+tableName+"("+ddlSql+") ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/"+tableName+"','{replica}') ";
    if (index != ""){
        SQL += "ORDER BY ("+index+")";
    }
    return SQL;
}