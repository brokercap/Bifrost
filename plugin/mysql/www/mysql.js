function doGetPluginParam(){
	var result = {data:{},status:false,msg:"failed"}

	var Table = $("#to_mysql_table").val();
    var Schema = $("#to_mysql_schema").val();
    var BatchSize = $("#MySQL_BatchSize").val();

    if (Schema == ""){
        result.msg = "请选择 数据库!";
        return result;
    }

    if (Table == ""){
        result.msg = "请选择 数据数据表!";
        return result;
    }

    if (BatchSize != "" && BatchSize != null && isNaN(BatchSize)){
        result.msg = "BatchSize must be int!"
        return result;
    }

	var PriKey = [];
	var Field = [];

    $.each($("#ToTableFieldsTable tr"),function () {
        var to_field_name = $(this).find("input[name=to_field_name]").val();
        var from_field_name = $(this).find("input[name=from_field_name]").val();

        var d       = {};
        d["ToField"]     = to_field_name;
        d["FromMysqlField"]  = from_field_name;
        if($(this).find("input[name=pri_checkbox]").is(':checked')) {
            if (to_field_name == "" || from_field_name == "") {
                result.msg = "PRI:" + to_field_name + " not empty";
                return result;
            }
            PriKey.push(d);
        }
        Field.push(d);
    });

    if(PriKey.length == 0){
        result.msg = "请选择一个字段为主键！";
        return result;
    }

    $.each($("#TableFieldsContair input:checkbox"),function(){
        $(this).attr("checked",false);
    });

    result.msg = "success";
    result.status = true;
    result.data["Field"]    = Field;
    result.data["PriKey"]   = PriKey;
    result.data["Schema"]   = Schema;
    result.data["Table"]    = Table;
    result.data["BatchSize"] = parseInt(BatchSize);

	return result;
}


function GetToSchameList() {
    $.get(
        "/bifrost/mysql/schemalist?toserverkey="+$("#addToServerKey").val(),
        function (d, status) {
            if (status != "success") {
                //console.log("/bifrost/clickhouse/schemalist?toserverkey="+$("#addToServerKey").val());
                return false;
            }
            var html = "<option value=''>请选择数据库</option>";
            for(i in d){
                var SchemaName = d[i];
                html += "<option value=\""+SchemaName+"\">"+SchemaName+"</option>";
            }
            $("#to_mysql_schema").html(html);
        }, 'json');
}

function GetToSchameTableList(schemaName) {
    $("#to_mysql_table").html("");
    if(schemaName == ""){
        $("#to_mysql_table").html("");

        return
    }
    $.get(
        "/bifrost/mysql/tablelist?toserverkey="+$("#addToServerKey").val()+"&schema="+schemaName,
        function (d, status) {
            if (status != "success") {
                return false;
            }

            var html = "<option value=''>请选择表</option>";
            for(i in d){
                var TableName = d[i];
                html += "<option value=\""+TableName+"\">"+TableName+"</option>";
            }
            $("#to_mysql_table").html(html);
        }, 'json');
}

function GetToTableDesc(schemaName,tableName) {
    $("#ToTableFieldsTable").html("");
    $.get(
        "/bifrost/mysql/tableinfo?toserverkey="+$("#addToServerKey").val()+"&schema="+schemaName+"&table_name="+tableName,
        function (d, status) {
            if (status != "success") {
                return false;
            }

            var fieldsMap = {};
            console.log(tableDataTypeMap);
            console.log(d)
            $.each($("#TableFieldsContair input"),function(){
                var field = $(this).val();
                console.log(field)
                fieldsMap[$(this).val()] = getTableFieldType(field.toLowerCase());
            });

            var html = "";
            for(i in d){

                var toField = "";
                var isPri = false;
                if(fieldsMap.hasOwnProperty(d[i].COLUMN_NAME.toLowerCase())){
                    toField = d[i].COLUMN_NAME;
                    if(fieldsMap[d[i].COLUMN_NAME].COLUMN_KEY == "PRI"){
                        isPri = true;
                    }
                }

                var htmlTr = "<tr id='to_field_name_"+d[i].COLUMN_NAME+"'>";
                htmlTr += "<td> <input type=\"text\"  value=\""+d[i].COLUMN_NAME+"\" type='"+d[i].DATA_TYPE+"' name=\"to_field_name\" disabled  class=\"form-control\" placeholder=\"\"></td>"
                htmlTr += "<td> <input type=\"text\" onfocus='ToMySQL_Input_onFocus(this)' id='mysql_filed_from_"+d[i].COLUMN_NAME+"' name=\"from_field_name\" value='"+toField+"' class=\"form-control\" placeholder=\"\"></td>";
                htmlTr += "<td> <input type='radio'"
                if(isPri){
                    htmlTr += " checked='checked' ";
                }
                htmlTr += " style='width: 20px; height: 20px' name='pri_checkbox' class=\"form-control ck_pri_checkbox\" /></td>";
                htmlTr += "</tr>";
                html += htmlTr;
            }
            $("#ToTableFieldsTable").html(html);
        }, 'json');
}

GetToSchameList();

var MySQL_OnFoucsInputId = "";

function ToMySQL_Input_onFocus(obj) {
    MySQL_OnFoucsInputId = $(obj).attr("id");
}


$("#TableFieldsContair").on("dblclick","p.fieldsname",function(){
    if (MySQL_OnFoucsInputId == ""){
        return false;
    }
    var fieldName = $(this).find("input").val();
    $("#"+MySQL_OnFoucsInputId).val($.trim(fieldName));
});

$("#TableFieldsContair p.fieldsname input:checkbox").click(
    function (){
        if (MySQL_OnFoucsInputId == ""){
            return false;
        }
        var fieldName = $(this).val();
        $("#"+MySQL_OnFoucsInputId).val($.trim(fieldName));
});