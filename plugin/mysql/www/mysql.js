function doGetPluginParam(){
	var result = {data:{},status:false,msg:"failed",batchSupport:false}

	var Table = $("#to_mysql_table").val();
    var Schema = $("#to_mysql_schema").val();
    var BatchSize = $("#MySQL_BatchSize").val();
    var NullTransferDefault = $("#MySQL_NullTransferDefault").val();
    var SyncMode = $("#MySQL_SyncMode").val();

    if( Table != "" && SyncMode == "NoSyncData" ) {
        result.msg = "不同步数据模式,只支持 自动表结构匹配表 模式!";
        result.batchSupport = true;
        return result;
    }

    if (BatchSize != "" && BatchSize != null && isNaN(BatchSize)){
        result.msg = "BatchSize must be int!"
        return result;
    }

	var PriKey = [];
	var Field = [];
    // 选择了指定目标表的情况下，并且非日志模式同步情况下，必须指定哪一个目标表字段为主键
    if ( Table != "" ) {
        $.each($("#ToTableFieldsTable tr"),function () {
            var to_field_name = $(this).find("input[name=to_field_name]").val();
            var from_field_name = $(this).find("input[name=from_field_name]").val();
            var d       = {};
            d["ToField"]     = to_field_name;
            //假如没有配置任务同步,则代表这个表采用，默认值
            if ( from_field_name == "" ){
                return;
            }
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
        if (PriKey.length == 0 && SyncMode != 'LogAppend') {
            result.msg = "请选择一个字段为主键！";
            return result;
        }
    }else{
        result.batchSupport = true;
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
    if (NullTransferDefault == "true"){
        result.data["NullTransferDefault"] = true;
    }else{
        result.data["NullTransferDefault"] = false;
    }
    result.data["SyncMode"] = SyncMode;
	return result;
}


function GetToSchameList() {
    $.get(
        "/bifrost/plugin/mysql/schemalist?ToServerKey="+$("#addToServerKey").val(),
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
        "/bifrost/plugin/mysql/tablelist?ToServerKey="+$("#addToServerKey").val()+"&SchemaName="+schemaName,
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
        "/bifrost/plugin/mysql/tableinfo?ToServerKey="+$("#addToServerKey").val()+"&SchemaName="+schemaName+"&TableName="+tableName,
        function (d, status) {
            if (status != "success") {
                return false;
            }

            var fieldsMap = {};
            $.each($("#TableFieldsContair input"),function(){
                fieldsMap[$(this).val().toLowerCase()] = getTableFieldType($(this).val());
            });

            var html = "";
            for(var i in d){
                var toField = "";
                var isPri = false;
                var tmpKey = d[i].COLUMN_NAME.toLowerCase();
                if(fieldsMap.hasOwnProperty(tmpKey)){
                    toField = fieldsMap[tmpKey].ColumnName;
                    if(fieldsMap[tmpKey].ColumnKey == "PRI"){
                        isPri = true;
                    }
                }

                if(toField == ""){
                    switch (tmpKey){
                        case "eventtype":
                        case "event_type":
                            toField = "{$EventType}";
                            break;
                        case "timestamp":
                            toField = "{$Timestamp}";
                            break;
                        case "binlogtimestamp":
                        case "binlog_timestamp":
                            toField = "{$BinlogTimestamp}";
                            break;
                        case "binlogfilenum":
                        case "binlog_filenum":
                            toField = "{$BinlogFileNum}";
                            break;
                        case "binlogposition":
                        case "binlog_position":
                        case "binlog_pos":
                            toField = "{$BinlogPosition}";
                            break;
                        default:
                            break;
                    }
                }

                var htmlTr = "<tr id='to_field_name_"+d[i].COLUMN_NAME+"'>";
                htmlTr += "<td> <input type=\"text\"  value=\""+d[i].COLUMN_NAME+"\" type='"+d[i].DATA_TYPE+"' name=\"to_field_name\" disabled  class=\"form-control\" placeholder=\"\"></td>"
                htmlTr += "<td> <input type=\"text\" onfocus='ToMySQL_Input_onFocus(this)' id='mysql_filed_from_"+d[i].COLUMN_NAME+"' name=\"from_field_name\" value='"+toField+"' class=\"form-control\" placeholder=\"\"></td>";
                htmlTr += "<td> <input type='radio'";
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

function showMySQLCreateSQL() {
    var param = getPluginFunctionParam();
    $.get(
        "/db/table/createsql?DbName="+param.DbName+"&SchemaName="+param.SchemaName+"&TableName="+param.TableName,
        function (d, status) {
            if (status != "success") {
                return false;
            }
            $("#showMySQLCreateSQL .modal-title").html("MySQL CreateSQL For Table </br>"+tableName);
            $("#showMySQLCreateSQL .modal-body").text(d);
            $("#showMySQLCreateSQL").modal('show');
        });
}

// 设置不过滤 sql 事件, sql 将会提交到 mysql 插件来
setPluginParamDefault("FilterQuery","false");