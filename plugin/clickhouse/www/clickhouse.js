var ckFieldDataMap = {};
function doGetPluginParam(){
	var result = {data:{},status:false,msg:"failed",batchSupport:false};

	var CkTable = $("#clickohuse_table").val();
    var CkSchema = $("#clickhouse_schema").val();
    var BatchSize = $("#CK_BatchSize").val();
    var SyncType = $("#clickhouse_sync_type").val();
    var NullNotTransferDefault = $("#clickhouse_NullNotTransferDefault").val();
    var LowerCaseTableNames = $("#clickhouse_LowerCaseTableNames").val();
    var AutoCreateTable = false;
    if (CkTable == ""){
        AutoCreateTable = true;
        SyncType = "insertAll";
        result.batchSupport = true;
    }

    if (BatchSize != "" && BatchSize != null && isNaN(BatchSize)){
        result.msg = "BatchSize must be int!";
        return result;
    }

	var PriKey = [];
	var Field = [];
	var eventTypeBool = false;
	var bifrostDataVersionBool = false;

	// 假如自动创建表的情况下，就不判断字段绑定关系了
	if ( AutoCreateTable == false ) {
        $.each($("#CKTableFieldsTable tr"), function () {
            var ck_field_name = $(this).find("input[name=ck_field_name]").val();
            //var ck_field_type = $(this).find("input[name=ck_field_name]").prop("ck_field_type");
            var ck_field_type = ckFieldDataMap[ck_field_name];
            //console.log("ck_field_name_input_:" + ck_field_name + " ck_field_type:" + ck_field_type);
            var mysql_field_name = $(this).find("input[name=mysql_field_name]").val();

            var d = {};
            d["CK"] = ck_field_name;
            d["MySQL"] = mysql_field_name;
            if ($(this).find("input[name=ck_pri_checkbox]").is(':checked')) {
                if (ck_field_name == "" || mysql_field_name == "") {
                    result.msg = "PRI:" + ck_field_name + " not empty";
                    return result;
                }
                PriKey.push(d);
            }
            Field.push(d);

            //$BifrostDataVersion 字段必须为 Int64 或者 UInt64 类型
            if (mysql_field_name == "{$BifrostDataVersion}" && ck_field_type.indexOf("Int64") != -1) {
                bifrostDataVersionBool = true;
            }
            if (mysql_field_name == "{$EventType}") {
                eventTypeBool = true;
            }
        });

        if (PriKey.length == 0) {
            result.msg = "请选择一个字段为主键！";
            return result;
        }
    }

    $.each($("#TableFieldsContair input:checkbox"),function(){
        $(this).attr("checked",false);
    });

    switch(SyncType){
        case "LogUpdate":
        case "Normal":
            if(bifrostDataVersionBool == false){
                if(!confirm("ClickHouse 表中没有配置 {$BifrostDataVersion} 标签的 字段！建议 ClickHouse 表中新增一个名为 [bifrost_data_version]  的字段后，刷新后配置再重试！请问是否继续 继续提交？！！！")){
                    result.msg = "请给 ClickHouse 表，新增字段 bifrost_data_version 后刷新再配置！";
                    return result;
                }
            }
        case "insertAll":
            if(AutoCreateTable == false && eventTypeBool == false){
                if(!confirm("日志模式-追加 模式，将会把 delete,update 也转成 insert 方式，追加到ClickHouse表中，当前没有字段配置 {$EventType} 标签表示数据是什么事件类型的！建议在表中新增一个名为 [bifrost_event_type] 的字段，刷新界面再重新配置！请问是否继续提交，还是 给 ClickHouse 表中 新增字段后再配置？！！！")){
                    result.msg = "请给 ClickHouse 表，新增字段 bifrost_event_type 后刷新再配置！";
                    return result;
                }
            }
            break;
    }
    result.msg = "success";
    result.status = true;
    result.data["Field"]            = Field;
    result.data["PriKey"]           = PriKey;
    result.data["CkSchema"]         = CkSchema;
    result.data["CkTable"]          = CkTable;
    result.data["BatchSize"]        = parseInt(BatchSize);
    result.data["SyncType"]         = SyncType;
    result.data["AutoCreateTable"]  = AutoCreateTable;
    result.data["LowerCaseTableNames"] = parseInt(LowerCaseTableNames);
    if (NullNotTransferDefault == "true"){
        result.data["NullNotTransferDefault"] = true;
    }else{
        result.data["NullNotTransferDefault"] = false;
    }
	return result;
}

function showClickHouseCreateSQL() {
    if($("#TableFieldsContair .fieldsname input").length == 0){
        alert("请先选择 MYSQL 表");
        return;
    }
    var tableName = getTableName();
    var sql = getClickHouseTableCreateSQL(tableName);

    $("#showClickHouseCreateSQL .modal-title").html("ClickHouse CreateSQL For Table </br>"+tableName);
    $("#showClickHouseCreateSQL .modal-body").text(sql);
    $("#showClickHouseCreateSQL").modal('show');
}

function getClickHouseTableCreateSQL(tableName) {
	var ddlSql = "";
	var index = "";
    var LowerCaseTableNames = $("#clickhouse_LowerCaseTableNames").val();
    var getFieldName = function (Name) {
        switch (LowerCaseTableNames) {
            case "0":
                return Name;
            case "1":
                return Name.toLowerCase();
            case "2":
                return Name.toUpperCase();
            default:
                return Name
        }
    };
    $.each($("#TableFieldsContair .fieldsname input"),
		function () {
            var data = getTableFieldType($(this).val());
            var getDDL = function (Type) {
                switch (Type){
                    case "Int8":
                    case "Int16":
                    case "Int32":
                    case "Int64":
                        if(data.COLUMN_TYPE.indexOf("unsigned") >0){
                            Type = "U"+Type;
                        }
                        break;
                    default:
                        break;
                }

                if(ddlSql == ""){
                    ddlSql = getFieldName(data.COLUMN_NAME) + " " + Type;
                }else{
                    ddlSql +="," + getFieldName(data.COLUMN_NAME) +" " + Type;
                }
                if(data.COLUMN_KEY == ""){
                    return
                }
                if (data.COLUMN_KEY != "PRI" && data.IS_NULLABLE != "NO" ){
                    return
                }
                if(index == ""){
                    index = getFieldName(data.COLUMN_NAME);
                }else{
                    index += "," + getFieldName(data.COLUMN_NAME);
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
                case "decimal":
                    return getDDL("String");
                    break;
                case "float":
                case "double":
                    return getDDL("Float64");
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
                    return getDDL("Int64");
                    break
                default:
                    return getDDL("String");
                    break;
            }
        }
	);

    ddlSql += ",binlog_event_type String,bifrost_data_version Int64";

    var SQL = "CREATE TABLE " + getFieldName(tableName) + "("+ddlSql+") ENGINE = MergeTree() ";
    if (index != ""){
        SQL += "ORDER BY ("+index+")";
    }
    return SQL;
}

function GetCkSchameList() {
    $.get(
        "/bifrost/plugin/clickhouse/schemalist?ToServerKey="+$("#addToServerKey").val(),
        function (d, status) {
            if (status != "success") {
                console.log("/bifrost/plugin/clickhouse/schemalist?ToServerKey="+$("#addToServerKey").val());
                return false;
            }
            var html = "<option value=''>自动创建CK库</option>";
            for(var i in d){
                var SchemaName = d[i];
                html += "<option value=\""+SchemaName+"\">"+SchemaName+"</option>";
            }
            $("#clickhouse_schema").html(html);
        }, 'json');
}

function GetCkSchameTableList(SchemaName) {
    $("#CKTableFieldsTable").html("");
    if(SchemaName == ""){
        $("#clickohuse_table").html("");

        return
    }
    $.get(
        "/bifrost/plugin/clickhouse/tablelist?ToServerKey="+$("#addToServerKey").val()+"&SchemaName="+SchemaName,
        function (d, status) {
            if (status != "success") {
                return false;
            }

            var html = "<option value=''>自动创建CK表</option>";
            for(var i in d){
                var TableName = d[i];
                html += "<option value=\""+TableName+"\">"+TableName+"</option>";
            }
            $("#clickohuse_table").html(html);
        }, 'json');
}

function GetCkTableDesc(SchemaName,TableName) {
    $("#CKTableFieldsTable").html("");
    ckFieldDataMap = {};
    $.get(
        "/bifrost/plugin/clickhouse/tableinfo?ToServerKey="+$("#addToServerKey").val()+"&SchemaName="+SchemaName+"&TableName="+TableName,
        function (d, status) {
            if (status != "success") {
                return false;
            }

            var fieldsMap = {};
            $.each($("#TableFieldsContair input"),function(){
                fieldsMap[$(this).val().toLowerCase()] = getTableFieldType($(this).val());
            });

            var html = "";
            if (d.length == 0){
                $("#CKTableFieldsTable").html(html);
                return;
            }
            for(var i in d){
                var toField = "";
                var isPri = false;
                var tmpKey = d[i].Name.toLowerCase();
                if(fieldsMap.hasOwnProperty(tmpKey)){
                    toField = fieldsMap[tmpKey].COLUMN_NAME;
                    if(fieldsMap[tmpKey].COLUMN_KEY == "PRI"){
                        isPri = true;
                    }
                }

                if(toField == ""){
                    switch (tmpKey){
                        case "eventtype":
                        case "event_type":
                        case "bifrost_event_type":
                        case "binlog_event_type":
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
                            toField = "{$BinlogFileNum}";
                            break;
                        case "binlogposition":
                            toField = "{$BinlogPosition}";
                            break;
                        case "bifrostdataversion":
                        case "bifrost_data_version":
                            toField = "{$BifrostDataVersion}";
                            break;
                        default:
                            break;
                    }
                }
                ckFieldDataMap[d[i].Name]=d[i].Type;
                var htmlTr = "<tr id='ck_field_name_"+d[i].Name+"'>";
                htmlTr += "<td> <input type=\"text\"  value=\""+d[i].Name+"\" name=\"ck_field_name\" disabled  class=\"form-control\" placeholder=\"\"></td>"
                htmlTr += "<td> <input type=\"text\" onfocus='ClickHouse_Input_onFocus(this)' id='ck_mysql_filed_from_"+d[i].Name+"' name=\"mysql_field_name\" value='"+toField+"' class=\"form-control\" placeholder=\"\"></td>";
                htmlTr += "<td> <input type='radio'"
                if(isPri){
                    htmlTr += " checked='checked' ";
                }
                htmlTr += " style='width: 20px; height: 20px' name='ck_pri_checkbox' class=\"form-control ck_pri_checkbox\" /></td>";
                htmlTr += "</tr>";
                html += htmlTr;
            }
            $("#CKTableFieldsTable").html(html);
        }, 'json');
}

function ClickHouse_Sync_Type_Change() {
    if( $("#clickhouse_sync_type").val()  == "Normal" ){
        $("#CK_BatchSize").val(1000);
    }else{
        $("#CK_BatchSize").val(5000);
    }
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

// 设置不过滤 sql 事件, sql 将会提交到 mysql 插件来
setPluginParamDefault("FilterQuery","false");