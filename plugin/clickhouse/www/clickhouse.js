function doGetPluginParam(){
	var result = {data:{},status:false,msg:"failed"}

	var CkTable = $("#clickohuse_table").val();
    var CkSchema = $("#clickhouse_schema").val();
    if (CkSchema == ""){
        result.msg = "请选择 ClickHouse 数据库!";
        return result;
    }

    if (CkTable == ""){
        result.msg = "请选择 ClickHouse 数据数据表!";
        return result;
    }

	var PriKey = "";
	var FieldsMap = {};

    $.each($("#CKTableFieldsTable tr"),function () {
        var ck_field_name = $(this).find("input[name=ck_field_name]").val();
        var mysql_field_name = $(this).find("input[name=mysql_field_name]").val();

        if($(this).find("input[name=ck_pri_checkbox]").is(':checked')) {
            if (ck_field_name == "" || mysql_field_name == "") {
                result.msg = "PRI:" + ck_field_name + " not empty";
                return result;
            }
            PriKey = ck_field_name;
        }
        FieldsMap[ck_field_name] = mysql_field_name;
    });

    if(PriKey == ""){
        result.msg = "请选择一个字段为主键！";
        return result;
    }

    $.each($("#TableFieldsContair input:checkbox"),function(){
        $(this).attr("checked",false);
    });

    result.msg = "success";
    result.status = true;
    result.data["Field"]    = FieldsMap;
    result.data["PriKey"]   = PriKey;
    result.data["CkSchema"] = CkSchema;
    result.data["CkTable"]  = CkTable;

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
                    return getDDL("Decimal("+data.NUMERIC_PRECISION+","+data.NUMERIC_SCALE+")");
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


    var SQL = "CREATE TABLE "+tableName+"("+ddlSql+") ENGINE = MergeTree() ";
    if (index != ""){
        SQL += "ORDER BY ("+index+")";
    }
    return SQL;
}

function GetCkSchameList() {
    $.get(
        "/bifrost/clickhouse/schemalist?toserverkey="+$("#addToServerKey").val(),
        function (d, status) {
            if (status != "success") {
                console.log("/bifrost/clickhouse/schemalist?toserverkey="+$("#addToServerKey").val());
                return false;
            }
            var html = "<option value=''>请选择数据库</option>";
            for(i in d){
                var SchemaName = d[i];
                html += "<option value=\""+SchemaName+"\">"+SchemaName+"</option>";
            }
            $("#clickhouse_schema").html(html);
        }, 'json');
}

function GetCkSchameTableList(schemaName) {
    $("#CKTableFieldsTable").html("");
    if(schemaName == ""){
        $("#clickohuse_table").html("");

        return
    }
    $.get(
        "/bifrost/clickhouse/tablelist?toserverkey="+$("#addToServerKey").val()+"&schema="+schemaName,
        function (d, status) {
            if (status != "success") {
                return false;
            }

            var html = "<option value=''>请选择表</option>";
            for(i in d){
                var TableName = d[i];
                html += "<option value=\""+TableName+"\">"+TableName+"</option>";
            }
            $("#clickohuse_table").html(html);
        }, 'json');
}

function GetCkTableDesc(schemaName,tableName) {
    $("#CKTableFieldsTable").html("");
    $.get(
        "/bifrost/clickhouse/tableinfo?toserverkey="+$("#addToServerKey").val()+"&schema="+schemaName+"&table_name="+tableName,
        function (d, status) {
            if (status != "success") {
                return false;
            }

            var fieldsMap = {};
            $.each($("#TableFieldsContair input"),function(){
                fieldsMap[$(this).val()] = getTableFieldType($(this).val());
            });

            var html = "";
            for(i in d){

                var toField = "";
                if(fieldsMap.hasOwnProperty(d[i].Name)){
                    toField = d[i].Name;
                }

                var htmlTr = "<tr id='ck_field_name_"+d[i].Name+"'>";
                htmlTr += "<td> <input type=\"text\"  value=\""+d[i].Name+"\" type='"+d[i].Type+"' name=\"ck_field_name\" disabled  class=\"form-control\" placeholder=\"\"></td>"
                htmlTr += "<td> <input type=\"text\" onfocus='ClickHouse_Input_onFocus(this)' id='ck_mysql_filed_from_"+d[i].Name+"' name=\"mysql_field_name\" value='"+toField+"' class=\"form-control\" placeholder=\"\"></td>";
                htmlTr += "<td> <input type='radio' style='width: 20px; height: 20px' name='ck_pri_checkbox' class=\"form-control ck_pri_checkbox\" /></td>";
                htmlTr += "</tr>";
                html += htmlTr;
            }
            $("#CKTableFieldsTable").html(html);
        }, 'json');
}

GetCkSchameList();

var CK_OnFoucsInputId = "";

function ClickHouse_Input_onFocus(obj) {
    CK_OnFoucsInputId = $(obj).attr("id");
}


$("#TableFieldsContair").on("dblclick","p.fieldsname",function(){
    if (CK_OnFoucsInputId == ""){
        return false;
    }
    var fieldName = $(this).find("input").val();
    $("#"+CK_OnFoucsInputId).val($.trim(fieldName));
});

$("#TableFieldsContair p.fieldsname input:checkbox").click(
    function (){
        if (CK_OnFoucsInputId == ""){
            return false;
        }
        var fieldName = $(this).val();
        $("#"+CK_OnFoucsInputId).val($.trim(fieldName));
});