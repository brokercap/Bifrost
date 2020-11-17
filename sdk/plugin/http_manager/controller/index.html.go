package controller

var IndexHtml = `

<!DOCTYPE html>
<html>

<head>

    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>mysqlLocalTest - Detail</title>
    <link rel="shortcut icon" href="favicon.ico">
    <link href="/css/bootstrap.min14ed.css?v=3.3.6" rel="stylesheet">
    <link href="/css/style.min862f.css?v=4.1.0" rel="stylesheet">
    <script src="/js/jquery.min.js?v=2.1.4"></script>
    <script src="/js/ajax.js?v=1.6.0"></script>
</head>

<body class="gray-bg top-navigation">

<div id="wrapper">
    <div id="page-wrapper" class="gray-bg">
        <div class="row border-bottom white-bg">
            <nav class="navbar navbar-static-top" role="navigation">
                <div class="navbar-header" style="float: left; width: auto">
                    <button aria-controls="navbar" aria-expanded="false" data-target="#navbar" data-toggle="collapse" class="navbar-toggle collapsed" type="button">
                        <i class="fa fa-reorder"></i>
                    </button>
                    <a href="http://www.xbifrost.com" target="_blank" class="navbar-brand">Bifrost</a>
                </div>
                <div class="navbar-collapse collapse" id="navbar" style="float: right; width: auto; margin-right: 0px">
                    <ul class="nav navbar-nav" id="header_nav">
                        <li class="active">
                            <a aria-expanded="false" role="button" href="/"> 返回首页</a>
                        </li>
                        <li class="dropdown">
                            <a  href="/db/index" class="dropdown-toggle" > 数据源 </a>
                        </li>
                        <li class="dropdown">
                            <a href="/toserver/index" class="dropdown-toggle"> 目标库列表 </a>
                        </li>
                        <li class="dropdown">
                            <a  href="/flow/index" class="dropdown-toggle"> 流量 </a>
                        </li>

                        <li class="dropdown">
                            <a  href="/table/synclist/index" class="dropdown-toggle"> 表同步列表 </a>
                        </li>

                        <li class="dropdown">
                            <a  href="/history/index" class="dropdown-toggle"> 全量任务 </a>
                        </li>
                        <li class="dropdown">
                            <a href="/plugin/index" class="dropdown-toggle"> 插件 </a>
                        </li>
                        <li class="dropdown">
                            <a href="/warning/config/index" class="dropdown-toggle"> 报警 </a>
                        </li>
                        <li class="dropdown">
                            <a href="/user/index" class="dropdown-toggle"> 用户管理 </a>
                        </li>
                        <li class="dropdown">
                            <a href="/docs" target="_blank" class="dropdown-toggle"> 文档 </a>
                        </li>


                    </ul>
                    <ul class="nav navbar-top-links navbar-right">
                        <li>
                            <a href="/logout">
                                <i class="fa fa-sign-out"></i> 退出
                            </a>
                        </li>
                    </ul>
                </div>
            </nav>
        </div>

    </div>
</div>

<script type="text/javascript">
function showOriginalData(obj,c){
    $(obj).parent().text(c);
}
function filterIpAndPort(c){
    var html = " <button data-toggle=\"button\" class=\"btn-sm btn-primary\" onclick='showOriginalData(this,\""+c+"\")' type=\"button\">show</button>\n"
    if(c.indexOf("@") != -1){
        document.write("***"+c.slice(c.indexOf("@")+1,c.length-5)+"***"+html)
    }else{
        document.write("***"+c.slice(5,c.length-5)+"***"+html)
    }
    return;
}
$(function(){
	$("#header_nav li a").each(function(){
		if(document.URL.indexOf($(this).attr("href")) != -1){
			$("#header_nav li").removeClass("active");
			$(this).parent().addClass("active");
		}
	 });
})
</script>


<link href="/css/plugins/bootstrap-table/bootstrap-table.min.css" rel="stylesheet" xmlns="http://www.w3.org/1999/html">
<div >
    <input type="hidden" value="mysqlLocalTest" id="DbName" />
    <div class="row">
        <div class="col-sm-2" id="MyWebLeft_1">

            <div class="ibox float-e-margins">
                <div class="ibox-title">
                    <h5>mysqlLocalTest - Schema List</h5>
                </div>
                <div class="ibox-content">
                    <div class="list-group" id="DatabaseListContair">
                     {{range $i, $v := .DataBaseList}}
                        <a class="list-group-item" id="Schema-{{$v}}">
                            <h3 class="list-group-item-heading">{{$v}}</h3>
                        </a>
                    {{end}}
                    </div>

                </div>
            </div>

        </div>
        <div class="col-sm-3" style=" padding-left: 0px;" id="MyWebLeft_2">
            <div class="ibox float-e-margins">
                <div class="ibox-title" style="position: relative">
                    <h5>Table List</h5>
                    <div  style="margin-left: 70px; margin-top:-10px; width: 50%" ><input type="text" class="form-control" placeholder="search" id="TableSearchName"></div>
                    <div id="MyWebHideBtn" onclick="showOrHideMyWeb();" style="position: absolute;right: 20px; top: 15px; font-weight:600; color:#666; cursor:pointer">Hide</div>
                </div>
                <style type="text/css">
                    .tableDiv{display:block; width:100%; height: 25px; position: relative}
                    .tableDiv .left{ display:block; float:left; width:79%; line-height:100%;word-wrap: break-word;  }
                    .tableDiv .right{ display:block; position: absolute; right: -10px ;top: 0px; width:78px; line-height: 25px;}
                    .tableDiv .right1{ display:block; position: absolute; right: 48px ;top: 0px; width:78px; line-height: 25px;}
                    .tableDiv .right2{ display:block; position: absolute; right: 105px ;top: 0px; width:78px; line-height: 25px;}
                    .tableDiv .right .button{ float: left;}
                    .tableDiv .right .check_input{ float: left;margin-left: 8px; margin-top: 2px}
                    .tableDiv .right1 .button{ float: left;}
                    .tableDiv .right2 .button{ float: left;}
                </style>
                <div class="ibox-content">
                    <div class="list-group" id="TableListContair">

                    </div>
                    <div id="bachTableDelOrAddBtnDiv">
                        <button data-toggle="button" class="btn-sm btn-danger" type="button" id="batchTableDeleteBtn">批量删除</button>
                        <button data-toggle="button" class="btn-sm btn-warning" id="batchTableChanneBindBtn" type="button">批量绑定通道</button>
                    </div>
                    <div style="line-height: 150%; padding: 10px">
                        <p>点击 ADD 按钮后再点击表名,让表名的背景变成绿色</p>
                        <p>多选框选中只能用于批量操作,要针对某一个表添加同步设置，请<strong style="color: #F00">点击</strong>表名，让表背景变成绿色</p>
                    </div>

                    <div id="batchDelOrAddTableResultDiv" style="padding-top: 20px; display:none"></div>
                </div>
            </div>
        </div>
        <div class="col-sm-7" style=" padding-left: 0px;" id="MyWebLeft_3">
            <div class="ibox float-e-margins">
                <div class="ibox-title" >
                    <h5>Table ToServer List
                        <span id="tableSelectShowDiv" style="display: none">
                        <a  href="#" id="tableFlowBtn" target="_blank"><button class="btn-sm btn-primary" type="button" style="margin-top: -8px">Flow</button></a>
                        &nbsp;
                        <button class="btn-sm btn-primary" id="historyAddBtn" type="button" style="margin-top: -8px" title="点击后可配置读取数据表的数据进行全量数据初始化">刷全量数据</button>
                        &nbsp;
                        <a href="#" id="tableHistoryListBtn">
                        <button class="btn-sm btn-primary" id="" type="button" style="margin-top: -8px">查看全量任务列表</button>
                        </a>
                        </span>
                    </h5>
                </div>
                <div class="ibox-content">

                    
                    <div class="example-wrap">
                        <div class="example">
                            <table id="tableToServerListContair" schema="" TableName="" DbName="" data-toggle="table" data-query-params="queryParams" data-mobile-responsive="true" data-height="auto" data-pagination="false" data-icon-size="outline">
                                <thead>
                                <tr>
                                    <th data-field="sliceid">sliceId/ID</th>
                                    <th data-field="PluginName">PluginType</th>
                                    <th data-field="ToServerKey">ToServerKey</th>
                                    <th data-field="FieldList">FieldList</th>
                                    <th data-field="Others">Others</th>
                                    <th data-field="PluginParam">PluginParam</th>
                                    <th data-field="Error">Error</th>
                                    <th data-field="op">op</th>
                                </tr>
                                </thead>
                            </table>
                        </div>
                    </div>
                    

                </div>


                <div class="ibox-content" id="addToServerContair">
                    <div class="row row-lg">

                        
                        <div class="col-md-7">
                            <div class="form-group">
                                <label class="col-sm-3 control-label">ToServerKey：</label>
                                <div class="col-sm-9" style="position: relative">
                                    <select class="form-control" name="addToServerKey" id="addToServerKey">
                                    
                                    {{range $k,$v := .ToServerList}}
                                        <option value="{{$k}}" pluginName="{{$v.PluginName}}" pluginVersion="{{$v.PluginVersion}}">{{$v.PluginName}} -- {{$k}}</option>
                                    {{end}}
                                    
                                    </select>
                                    <span class="help-block m-b-none"></span>
                                    <div style="position: absolute; top: 0px; right: -30px;">
                                        <a href="#" target="_blank" id="addToServerKeyDoc"><button class="btn-sm btn-primary" type="button" style="padding: 5px">DOC</button></a>
                                    </div>
                                </div>
                            </div>

                            <div id="plugin_param_div" style="padding:10px 0px">

                            </div>

                            <div class="form-group">
                                <label class="col-sm-3 control-label">MustBeSuccess：</label>
                                <div class="col-sm-9">
                                    <select class="form-control" name="MustBeSuccess" id="MustBeSuccess">
                                        <option value="true" selected="selected">True</option>
                                        <option value="false">False</option>
                                    </select>
                                    <p class="help-block m-b-none">True: 插件返回失败，自动重试，直到成功为止</p>
                                </div>
                            </div>

                            <div class="form-group">
                                <label class="col-sm-3 control-label">FilterQuery：</label>
                                <div class="col-sm-9">
                                    <select class="form-control" name="FilterQuery" id="FilterQuery">
                                        <option value="true" selected="selected">True</option>
                                        <option value="false">False</option>
                                    </select>
                                    <p class="help-block m-b-none">True: 将过滤sql 事件，不提供给插件层处理，False: 由插件层自行决定怎么处理</p>
                                </div>
                                
                            </div>
                            
                            <div class="form-group">
                                <label class="col-sm-3 control-label">FilterUpdate：</label>
                                <div class="col-sm-9">
                                    <select class="form-control" name="FilterUpdate" id="FilterUpdate">
                                        <option value="true" selected="selected">True</option>
                                        <option value="false">False</option>
                                    </select>
                                    <p class="help-block m-b-none">True: update事件，所选字段内容都没有变更情况下，不进行推送，False: 不管字段有没有更新，全部都会推送</p>
                                </div>
                                
                            </div>

                            <div class="form-group">
                                <label class="col-sm-3 control-label">&nbsp;</label>
                                <div class="col-sm-9" style="padding-top: 15px;">
                                    <button data-toggle="button" class="btn-sm btn-primary" id="addToServerBtn" type="button">提交</button>
                                    <button data-toggle="button" class="btn-sm btn-success" id="batchAddToServerBtn" type="button">批量提交</button>
                                    <p>&nbsp;</p>
                                    <p style="color:#F00">批量提交，将会提交到多选框选中并且已绑定Channel的表里,批量提交,字段不能选,默认全选</p>
                                </div>
                            </div>

                            <div class="form-group">
                                <label class="col-sm-3 control-label">&nbsp;</label>
                                <div class="col-sm-9" style="padding-top: 15px;" id="batchAddToServerResultDiv">

                                </div>
                            </div>
                        </div>

                        
                        

                        <div class="col-md-5">
                            <label class="col-sm-2 control-label">Fields：</label>
                            <div class="col-sm-10" id="TableFieldsContair">

                            </div>
                        </div>

                        

                    </div>
                </div>
            </div>


        </div>
    </div>



<div class="modal inmodal fade" id="showLikeTable" tabindex="-1" role="dialog"  aria-hidden="true">
    <div class="modal-dialog">
        <div class="modal-content">
            <div class="modal-header">
                <button type="button" class="close" data-dismiss="modal"><span aria-hidden="true">&times;</span><span class="sr-only">Close</span></button>
                <h3 class="modal-title" id="showLikeTable_Title"></h3>
            </div>
            <div class="modal-body" id="showLikeTable_Body">

            </div>
            <div class="modal-footer">
                <button type="button" class="btn-sm btn-white" data-dismiss="modal">关闭</button>
            </div>
        </div>
    </div>
</div>
<script type="text/javascript">
    function ShowLikeTableNames(tableName,ignoreTables) {
        var tableNames = getTablesByLikeName(tableName);
        ignoreTables = ignoreTables+",";
        $("#showLikeTable_Title").text(tableName);
        var html = "";
        for (var name of tableNames.split(";")) {
            if( ignoreTables.indexOf(name+",") < 0 ){
                html += "<p>"+name+"</p>";
            }
        }
        $("#showLikeTable_Body").html(html);
        $("#showLikeTable").modal('show');
    }
</script>


    <script src="/js/bootstrap.min.js?v=3.3.6"></script>
    <script src="/js/plugins/bootstrap-table/bootstrap-table.min.js"></script>
    <script src="/js/md5.min.js"></script>
    <script type="text/javascript">

        
        var DbName = "mysqlLocalTest";
        
        var SchemaName = "";
        
        var TableName = ""
        
        var OnFoucsInputId = "";
        
        var tableDataTypeMap = {};
        
        var TableMap = new Map();

        
        var DataBaseMap = new Map();

        function getDbName() {
            return DbName;
        }
        function setSchemaName(name) {
            SchemaName = name
        }

        function getSchemaName() {
            return SchemaName;
        }

        function setTableName(name) {
            TableName = name
        }

        function getTableName() {
            return TableName
        }

        function getPluginFunctionParam() {
            var param = {
                DbName : getDbName(),
                SchemaName: getSchemaName(),
                TableName: getTableName(),
            }
            return param;
        }

        function getTableFieldType(field) {
            return tableDataTypeMap[field];
        }

        function clearTableDataMap() {
            tableDataTypeMap = {};
        }
        function setTableFieldType(field,dataType) {
            tableDataTypeMap[field] = dataType;
        }

        function setPluginParamDefault(key,value) {
            if (key == undefined || key == null){
                $("#MustBeSuccess").val("true");
                $("#FilterQuery").val("true");
                $("#MustBeSuccess").val("true");
                return;
            }
            switch (key){
                case "FilterUpdate":
                    if (value != false && value != "false"){
                        $("#FilterUpdate").val("true");
                    }else{
                        $("#FilterUpdate").val("false");
                    }
                    break;
                case "FilterQuery":
                    if (value != false && value != "false"){
                        $("#FilterQuery").val("true");
                    }else{
                        $("#FilterQuery").val("false");
                    }
                    break;
                case "MustBeSuccess":
                    if (value != false && value != "false"){
                        $("#MustBeSuccess").val("true");
                    }else{
                        $("#MustBeSuccess").val("false");
                    }
                    break;
                default:
                    break;
            }
            return;
        }

        function showOrHideMyWeb(){
            var status = $("#MyWebHideBtn").attr("status");
            if (status == "hide"){
                $("#MyWebLeft_2").removeClass("col-sm-1").addClass("col-sm-3");
                $("#MyWebLeft_3").removeClass("col-sm-11").addClass("col-sm-7");
                $("#MyWebLeft_1").show();
                $("#MyWebHideBtn").attr("status","show");
            }else{
                $("#MyWebLeft_1").hide();
                $("#MyWebLeft_2").removeClass("col-sm-3").addClass("col-sm-1");
                $("#MyWebLeft_3").removeClass("col-sm-7").addClass("col-sm-11");
                $("#MyWebHideBtn").attr("status","hide");
            }
        }

        function showBatchDelOrAddBtn() {
            $("#bachTableDelOrAddBtnDiv").show();
        }
        $("#bachTableDelOrAddBtnDiv").hide();

        function batchDelOrAddTableResultFun(content,type) {
            if(type == -1){
                $("#batchDelOrAddTableResultDiv").show();
            }
            if (type == 0 || type == -1 ){
                $("#batchDelOrAddTableResultDiv").html("<p>"+content+"</p>");
            }else{
                $("#batchDelOrAddTableResultDiv").append("<p>"+content+"</p>");
            }
            if(type == -2){
               
            }
        }

        $("#batchTableChanneBindBtn").click(
            function () {
                var TableNames = "";
                $("#TableListContair .check_input input[type='checkbox']:checked").each(function (index, item) {
                    if($(this).parent().parent().find(".button button").text()=="ADD"){
                        TableNames += $(this).val()+";"
                    }
                });
                showAddTable(TableNames);
            }
        );

        $("#batchTableDeleteBtn").click(
                function () {
                    var TableNames = "";
                    $("#TableListContair .check_input input[type='checkbox']:checked").each(function (index, item) {
                        if($(this).parent().parent().find(".button button").text()=="DEL"){
                            TableNames += $(this).val()+";"
                        }
                    });
                    DelTable(TableNames);
                }
        );

        function TransferLikeTableReq(TableName) {
            if (TableName == "AllTables") {
                return ".*";
            }else{
                var reqTableName = TableName;
                
                if (reqTableName.charAt(0) == "*"){
                    reqTableName = "(.*)"+reqTableName.substr(1,reqTableName.length-1);
                }
                
                if ((reqTableName.indexOf("(.*)") != 0) && reqTableName.charAt(0) != "^") {
                    reqTableName = "^"+reqTableName
                }
                
                
                if (reqTableName.charAt(reqTableName.length-1) == "*"){
                    reqTableName = reqTableName.substr(0,reqTableName.length-1)+"(.*)";

                }
                
                if (reqTableName.length >= 4 && reqTableName.substr(reqTableName.length-4,reqTableName.length) != "(.*)" && reqTableName.charAt(reqTableName.length-1) != "$"){
                    reqTableName = reqTableName + "$";
                }
                return reqTableName;
            }
        }

        function getTablesByLikeName(TableName) {
            var TableNames = "";
            var reg;
            if ( TableName != "AllTables" && TableName.indexOf("*") == -1 ){
                return "";
            }
            reg = new RegExp(TransferLikeTableReq(TableName));
            $('#TableListContair').find(":input[name=table_check_name]").each(function(){
                if( $(this).val() != "AllTables" && $(this).val().indexOf("*") == -1 ){
                    var regResult = $(this).val().match(reg);
                    if ( regResult != null && regResult != undefined){
                        TableNames += $(this).val()+";";
                    }
                }
            });
            return TableNames;
        }

        function DelTable(TableNames){
            if(TableNames == ""){
                return false;
            }
            if (!confirm("确定删除 [ "+TableNames+" ] ?删除后不能恢复!!!")){
                return false;
            }
            var DbName = $("#DbName").val();
            var SchemaName = $("#DatabaseListContair a.active").find("h3").text();
            var url = "/table/del";
            var arr = TableNames.split(";");
            batchDelOrAddTableResultFun("开始删除",-1);
            for( var i in arr) {
                var TableName = arr[i];
                if ( TableName == "" ){
                    continue;
                }
                var callback = function (data) {
                    if (data.status) {
                        var tableDivId = md5(SchemaName + "_-" + TableName);
                        var html = '<button data-toggle="button" class="btn-warning btn-sm" type="button" onClick="showAddTable(\'' + TableName + '\')">ADD</button>';
                        $("#" + tableDivId + " .right .button").html(html);
                        DataBaseMap.delete(SchemaName);
                    }
                    batchDelOrAddTableResultFun(TableName +" DEL Result:"+data.msg,1);
                };
                var ajaxParam =  {DbName: DbName, SchemaName: SchemaName, TableName: TableName};
                Ajax("POST",url, ajaxParam,callback,false);
            }
            batchDelOrAddTableResultFun("删除完成",-2);
        }
        function ChangeTableFlowBtnHref(SchemaName,TableName){
            $("#tableFlowBtn").attr("href","/flow/index?DbName="+DbName+"&schema="+SchemaName+"&TableName="+TableName);
            $("#tableHistoryListBtn").attr("href","/history/index?DbName="+DbName+"&schema="+SchemaName+"&TableName="+TableName);
            $("#tableSelectShowDiv").show();
        }

        
        function table_check_input(id) {
            var idNew = "#"+id+" .check_input input[name=table_check_name]";
            if($(idNew).is(':checked')){
                $(idNew).prop("checked", false);
            }else{
                $(idNew).prop("checked", true);
            }
        }

        function GetTableToServerList(SchemaName,TableName){
            var key = md5(SchemaName+"_-"+TableName);
            if ($("#"+key+" .right button").text() == "ADD"){
                table_check_input(key);
                return  false;
            }
            setTableName(TableName);
            $("#TableListContair a").removeClass("active");
            $("#"+key).parent("a").addClass("active");
            ChangeTableFlowBtnHref(SchemaName,TableName);
            var url = "/table/toserver/list";
            var callback = function (data) {
                var e = [];
                $.each(data,function(index,v){
                    var fields = '';
                    if (v.FieldList != null) {
                        fields = v.FieldList.toString();
                    }
                    var PluginHtml = "";
                    for(var key in v.PluginParam){
                        if (typeof v.PluginParam[key] == "object"){
                            PluginHtml += "<p>"+key+":"+ JSON.stringify(v.PluginParam[key]) +"</p>";
                        }else{
                            PluginHtml += "<p>"+key+":"+v.PluginParam[key]+"</p>";
                        }
                    }
                    var ErrorHtml = "";
                    if (v.Error != "" && v.Error != null){
                        var ErrData = "";
                        if ( v.ErrorWaitData != null ) {
                            ErrData = JSON.stringify(v.ErrorWaitData);
                        }
                        ErrorHtml = "<p>Err:"+v.Error+"</p><p>Data:"+ErrData+"</p><p><button data-toggle='button' class='btn-sm btn-primary' onClick='DealWaitErr(this,"+v.ToServerID+")' type='button'>Skip</button></p>";
                    }
                    var op = "<p>"+v.Status+"</p>";
                    if (v.Status == "stopped"){
                        op += '<p><button data-toggle="button" class="btn-sm btn-primary btn-sm" type="button" onClick="UpdateTableToServerStatus(this,'+v.ToServerID+','+index+',\'start\')">START</button></p>';
                        op += '<p><button data-toggle="button" class="btn-sm btn-danger btn-sm" type="button" onClick="UpdateTableToServerStatus(this,'+v.ToServerID+','+index+',\'del\')">DEL</button></p>';
                    }else if (v.Status == "running" || v.Status == ""){
                        op += '<p><button data-toggle="button" class="btn-sm btn-warning btn-sm" type="button" onClick="UpdateTableToServerStatus(this,'+v.ToServerID+','+index+',\'stop\')">STOP</button></p>';
                        op += '<p><button data-toggle="button" class="btn-sm btn-danger btn-sm" type="button" onClick="UpdateTableToServerStatus(this,'+v.ToServerID+','+index+',\'del\')">DEL</button></p>';
                    }else{
                        op = v.Status;
                    }
                    var others = "";

                    others += "<p>MustBeSuccess: "+v.MustBeSuccess+"</p><p>FilterQuery: "+v.FilterQuery+"</p><p>FilterUpdate: "+v.FilterUpdate+"</p>";
                    others += "<p title=\"最后一个成功处理的位点\">BinlogFileNum: "+v.BinlogFileNum+"</p><p>BinlogPos: "+v.BinlogPosition+"</p>";
                    others += "<p>LastBinlogFileNum: "+v.LastBinlogFileNum+"</p>";
                    others += "<p title=\"队列最后一个位点\">LastBinlogPosition: "+v.LastBinlogPosition+"</p>";
                    others += "<p title=\"文件队列是否启用\">FileQueueStatus: "+v.FileQueueStatus+"</p>";
                    if ("QueueMsgCount" in v) {
                        others += "<p title=\"内存队列堆积多少条数据待同步\">QueueMsgCount: " + v.QueueMsgCount + "</p>";
                    }
                    e.push({
                                sliceid:index+"/"+v.ToServerID,
                                PluginName:v.PluginName,
                                ToServerKey:v.ToServerKey,
                                FieldList:"<p style='max-width: 200px;word-wrap:break-word'>"+fields+"</p>",
                                Others:others,
                                PluginParam:PluginHtml,
                                Error:ErrorHtml,
                                op:op,
                            }
                    );
                });
                $("#tableToServerListContair").attr("DbName",DbName);
                $("#tableToServerListContair").attr("schema",SchemaName);
                $("#tableToServerListContair").attr("TableName",TableName);
                $("#tableToServerListContair").bootstrapTable("load",e);

                var TableName_alias = "";
                if ( TableName.indexOf("*") != -1 ){
                    var tableNames = getTablesByLikeName(TableName);
                    if ( tableNames != "" ) {
                        TableName_alias = tableNames.split(";")[0];
                    }
                    if( TableName_alias == "" ){
                        alert("没有匹配到表名！请检查配置是否正确，或者创建一个符合当前规则表，再进行选择配置！")
                        return false;
                    }
                }else{
                    TableName_alias = TableName
                }
                GetTableFields(SchemaName,TableName_alias);
            };

            var ajaxParam = {DbName:DbName,SchemaName:SchemaName,TableName:TableName};
            Ajax("GET",url, ajaxParam,callback,true);
        }

        function UpdateTableToServerStatus(obj,ToServerID,index,status){
            var url = "";
            var opName = "";
            switch (status){
                case "stop":
                    if (!confirm("确定暂停第 [ "+ index +" ] 条记录？")){
                        return false;
                    }
                    url = "/table/toserver/stop";
                    opName = "暂停";
                    break;
                case "del":
                    if (!confirm("确定删除第 [ "+ index +" ] 条记录？删除将不能恢复？！！！")){
                        return false;
                    }
                    url = "/table/toserver/del";
                    opName = "删除";
                    break;
                case "start":
                    url = "/table/toserver/start";
                    opName = "启动";
                    break;
                default:
                    return;
            }
            var DbName = $("#tableToServerListContair").attr("DbName");
            var SchemaName = $("#tableToServerListContair").attr("schema");
            var TableName = $("#tableToServerListContair").attr("TableName");
            var callback = function (data) {
                if (!data.status){
                    alert(data.msg);
                    return;
                }
                alert(opName+"成功!");
                GetTableToServerList(SchemaName,TableName);
            };
            var ajaxParam = {DbName:DbName,SchemaName:SchemaName,TableName:TableName,Index:index,ToServerId:ToServerID};
            Ajax("POST",url, ajaxParam,callback,true);
        }

        function GetTableFields(SchemaName,TableName){
            var key = SchemaName+"_-"+TableName;
            if (!TableMap.has(key) || TableMap.get(key) == undefined){
                var url = '/db/table/fields';

                var callback = function (data) {
                    TableMap.set(key,data);
                    showFieldsList(key);
                };
                var ajaxParam = {DbName:DbName,SchemaName:SchemaName,TableName:TableName};
                Ajax("GET",url, ajaxParam,callback,true);

            }else{
                showFieldsList(key);
            }
        }
        function showFieldsList(key) {
            var html = "";
            clearTableDataMap();
            TableMap.get(key).forEach(function (value, k, map) {
                var phtml = "";
                setTableFieldType(value.COLUMN_NAME,value)
                phtml +='<input type="checkbox" style="width: 20px; height: 20px;" title="'+value.COLUMN_COMMENT+'" value="'+value.COLUMN_NAME+'">';
                phtml += "&nbsp;&nbsp;"+value.COLUMN_NAME;
                if (value.COLUMN_KEY == "PRI"){
                    phtml += " (PRI)";
                }
                html += '<p class="fieldsname" style="font-size: 16px; cursor: pointer">&nbsp;'+phtml+' </p>'
            });
            $("#TableFieldsContair").html(html);
        }

        function showSchemaTableList(id){
            $("#DatabaseListContair a").removeClass("active");
            $("#"+id).addClass("active");
            var SchemaName = $("#"+id).find("h3").text();
            var DbName = $("#DbName").val();
            var url = "/db/table/list";
            setSchemaName(SchemaName);
            var showTableList = function(data){
                $("#TableListContair").html("");
                $.each(data,function(index,v) {
                    var html = "";
                    var title = "";
                    if (v.ChannelName != "") {
                        title = " title='Bind Channel : " + v.ChannelName;
                        if (v.IgnoreTable != "") {
                            title += " IgnoreTable: "+v.IgnoreTable;
                        }
                        title += "'";
                    }
                    var tableDivId =  md5(SchemaName + '_-' + v.TableName);
                    html += '<a class="list-group-item"><div class="tableDiv" title="' + v.TableName + '" id="' + tableDivId + '">';
                    html += '<h5 class="left" ' + title + ' onClick="GetTableToServerList(\'' + SchemaName + '\',\'' + v.TableName + '\')">' + v.TableName + '</h5>';
                    if (v.TableType.toUpperCase().indexOf("VIEW") != -1) {
                        html += '<div class="right1">';
                        html += '<div class="button"><button data-toggle="button" class="btn-sm btn-success" type="button">视图</button></div>';
                        html += '</div>';
                    }
                    if (v.TableType.toUpperCase().indexOf("LIKE") != -1) {
                        if (v.AddStatus == true){
                            html += '<div class="right2">';
                            html += '<div class="button"><button data-toggle="button" class="btn-sm btn-warning" type="button" onclick="showUpdateTable(\''+DbName+'\',\''+SchemaName+'\',\''+v.TableName+'\',\''+v.IgnoreTable+'\')">修改</button></div>';
                            html += '</div>';
                        }
                        html += '<div class="right1">';
                        html += '<div class="button"><button data-toggle="button" class="btn-sm btn-success" type="button" onclick="ShowLikeTableNames(\''+v.TableName+'\',\''+v.IgnoreTable+'\')">多表</button></div>';
                        html += '</div>';
                    }
                    html += '<div class="right">';
                    if (v.AddStatus == false){
                        html+= '<div class="button"><button data-toggle="button" class="btn-sm btn-warning" type="button" onClick="showAddTable(\''+v.TableName+'\')">ADD</button></div>';
                    }else{
                        html+= '<div class="button"><button data-toggle="button" class="btn-sm btn-danger" type="button" onClick="DelTable(\''+v.TableName+'\')">DEL</button></div>';
                    }
                    html += "<div class='check_input'> <input type='checkbox' name='table_check_name' value='"+v.TableName+"' style='width: 20px; height: 20px;' /></div>";
                    html += '</div>';
                    html +=	'</div></a>';
                    $("#TableListContair").append(html);
                });
            }
            if (!DataBaseMap.has(SchemaName) || DataBaseMap.get(SchemaName) == undefined){

                var callback = function (data) {
                    DataBaseMap.set(SchemaName,data);
                    showTableList(data);
                };
                var ajaxParam = {DbName:DbName,SchemaName:SchemaName};
                Ajax("GET",url, ajaxParam,callback,true);
            }else{
                showTableList(DataBaseMap.get(SchemaName));
            }
            showBatchDelOrAddBtn();
        }

        $(function(){
            $("#plugin_param_div").on("click",":text,textarea",function(){
                OnFoucsInputId = $(this).attr("id");
            });

            $("#DatabaseListContair a").click(
                    function(){
                        $("#TableSearchName").val("");
                        showSchemaTableList($(this).attr("id"));
                    }
            );
            var doChangeToServer = function(){
                if($("#addToServerKey").val()==null){
                    if(confirm("目标库地址为空，是否跳转到 添加 目标库 ？")){
                        window.location.href = "/toserver/index";
                        return;
                    }else{
                        return;
                    }
                }
                
                setPluginParamDefault();
                var pluginName = $("#addToServerKey").find("option:selected").attr("pluginName");
                var pluginVersion = $("#addToServerKey").find("option:selected").attr("pluginversion");
                $("#plugin_param_div").load("/plugin/"+pluginName+"/www/"+pluginName+".html?v="+pluginVersion);
                $.getScript("/plugin/"+pluginName+"/www/"+pluginName+".js?v="+pluginVersion,function(){});
                $("#addToServerKeyDoc").attr("href","/docs?plugin="+pluginName+"#pluginDocName");
            }
            
            $("#addToServerKey").change(function(){
                doChangeToServer();
            });
            doChangeToServer();

            $("#TableFieldsContair").on("dblclick","p.fieldsname",function(){
                if (OnFoucsInputId == ""){
                    return false;
                }
                var fieldName = $(this).find("input").val();
                $("#"+OnFoucsInputId).val($("#"+OnFoucsInputId).val()+"{$"+($.trim(fieldName))+"}");
            });

            $("#addToServerBtn").click(
                    function(){
                        if($("#addToServerKey").val()==null){
                            if(confirm("目标库地址为空，是否跳转到 添加 目标库 ？")){
                                window.location.href = "/toserver/list";
                                return false;
                            }else{
                                return false;
                            }
                        }
                        var p = doGetPluginParam(getPluginFunctionParam());;
                        if (getTableName() == "AllTables"){
                            if(  p.batchSupport != true){
                                alert("当前插件配置不支持 批量 设置!")
                                return false;
                            }
                        }
                        if (p.status == false){
                            alert(p.msg);
                            return false;
                        }
                        var DbName = $("#tableToServerListContair").attr("DbName");
                        var SchemaName = $("#tableToServerListContair").attr("schema");
                        var TableName = $("#tableToServerListContair").attr("TableName");
                        if (TableName == ""){
                            alert("selected table please!");
                            return false;
                        }
                        var MustBeSuccess = $("#MustBeSuccess").val();
                        if (MustBeSuccess == "true"){
                            MustBeSuccess = true;
                        }else{
                            MustBeSuccess = false;
                        }
                        var addToServerKey = $("#addToServerKey").val();
                        var fieldlist = [];
                        $.each($("#TableFieldsContair input:checkbox:checked"),function(){
                            fieldlist.push($(this).val());
                        });
						var FilterQuery = $("#FilterQuery").val();
                        if (FilterQuery == "true"){
                            FilterQuery = true;
                        }else{
                            FilterQuery = false;
                        }
						var FilterUpdate = $("#FilterUpdate").val();
                        if (FilterUpdate == "true"){
                            FilterUpdate = true;
                        }else{
                            FilterUpdate = false;
                        }
                        var pluginName = $("#addToServerKey").find("option:selected").attr("pluginName");
                        var url = '/table/toserver/add';
                        var data = {
                            DbName:DbName,
                            SchemaName:SchemaName,
                            TableName:TableName,
                            ToServerKey:addToServerKey,
                            PluginName:pluginName,
                            MustBeSuccess:MustBeSuccess,
							FilterQuery:FilterQuery,
							FilterUpdate:FilterUpdate,
                            FieldList:fieldlist,
                            PluginParam:p.data,
                        };
                        var callback = function (data) {
                            if(!data.status){
                                alert(data.msg);
                                return false;
                            }
                            GetTableToServerList(SchemaName,TableName);
                            doChangeToServer();
                        };
                        Ajax("POST",url, data,callback,false);
                    }
            );


            $("#batchAddToServerBtn").click(
                    function(){
                        if($("#addToServerKey").val()==null){
                            if(confirm("目标库地址为空，是否跳转到 添加 目标库 ？")){
                                window.location.href = "/toserver/list";
                                return;
                            }else{
                                return;
                            }
                        }
                        var obj = $(this);
                        if ($(obj).text() == "正在提交"){
                            return false;
                        }
                        var p = doGetPluginParam(getPluginFunctionParam());
                        if(  p.batchSupport != true){
                            alert("当前插件配置不支持 批量 设置!")
                            return false;
                        }
                        if (p.status == false){
                            alert(p.msg);
                            return false;
                        }
                        var DbName = $("#tableToServerListContair").attr("DbName");
                        var SchemaName = $("#tableToServerListContair").attr("schema");
                        var tableArr = [];

                        $("#TableListContair .check_input input[type='checkbox']:checked").each(function (index, item) {
                            if($(this).parent().parent().find(".button button").text()=="DEL"){
                                tableArr.push($(this).val());
                            }
                        });

                        if (tableArr.length == 0){
                            alert("请先选择 需要同步的 表");
                            return false;
                        }
                        var fieldlist = [];
                        $.each($("#TableFieldsContair input:checkbox:checked"),function(){
                            fieldlist.push($(this).val());
                        });
                        if (fieldlist.length > 0){
                            alert("批量提交不能选择字段");
                            return false;
                        }
                        var MustBeSuccess = $("#MustBeSuccess").val();
                        if (MustBeSuccess == "true"){
                            MustBeSuccess = true;
                        }else{
                            MustBeSuccess = false;
                        }
                        var addToServerKey = $("#addToServerKey").val();
                        var fieldlist = [];
                        $.each($("#TableFieldsContair input:checkbox:checked"),function(){
                            fieldlist.push($(this).val());
                        });
                        var FilterQuery = $("#FilterQuery").val();
                        if (FilterQuery == "true"){
                            FilterQuery = true;
                        }else{
                            FilterQuery = false;
                        }
                        var FilterUpdate = $("#FilterUpdate").val();
                        if (FilterUpdate == "true"){
                            FilterUpdate = true;
                        }else{
                            FilterUpdate = false;
                        }
                        var pluginName = $("#addToServerKey").find("option:selected").attr("pluginName");
                        var url = '/table/toserver/add';
                        var data = {
                            DbName:DbName,
                            SchemaName:SchemaName,
                            TableName:TableName,
                            ToServerKey:addToServerKey,
                            PluginName:pluginName,
                            MustBeSuccess:MustBeSuccess,
                            FilterQuery:FilterQuery,
                            FilterUpdate:FilterUpdate,
                            FieldList:fieldlist,
                            PluginParam:p.data,
                        }
                        $(obj).text("正在提交");
                        $("#batchAddToServerResultDiv").html("");
                        var resultFun = function (content) {
                            $("#batchAddToServerResultDiv").append("<p>"+content+"</p>");
                        }
                        resultFun("开始提交");
                        for(var i in tableArr) {
                            data.TableName = tableArr[i];

                            var callback = function (dataResult) {
                                if(dataResult.status){
                                    resultFun(tableArr[i]+" success");
                                }else{
                                    resultFun(tableArr[i]+ " "+ dataResult.msg);
                                }
                            };
                            Ajax("POST",url, data,callback,false);
                        }
                        resultFun("执行完成");
                        $(obj).text("批量提交");
                    }
            );

        });

        function DealWaitErr(obj,ToServerID){
            var thisButton = $(obj);
            var index0 = $(obj).parent().parent().parent("tr").children().eq(0).html();
            var DbName = $("#tableToServerListContair").attr("DbName");
            var SchemaName = $("#tableToServerListContair").attr("schema");
            var TableName = $("#tableToServerListContair").attr("TableName");
            var index = index0.split("/")[0];
            var url = "/table/toserver/deal";

            var callback = function (dataResult) {
                if(!dataResult.status){
                    alert(dataResult.msg);
                    return false;
                }
                $(thisButton).parent().parent().html("");
            };
            Ajax("POST",url,{DbName: DbName,SchemaName:SchemaName,TableName:TableName,ToServerId:parseInt(ToServerID),Index:parseInt(index)},callback,false);
        }


        function getQueryString(name) {
            var reg = new RegExp("(^|&)" + name + "=([^&]*)(&|$)", "i");
            var r = window.location.search.substr(1).match(reg);
            if (r != null) return unescape(r[2]);
            return null;
        }

        function init(){
            var querySchemaName = getQueryString("schema");
            var queryTableName = getQueryString("TableName");
            if(querySchemaName == null) {
                
                $.each($("#DatabaseListContair a"),function(){
                    var id = $(this).attr("id");
                    if(querySchemaName != null){
                        return;
                    }
                    
                    if(id == "Schema-information_schema" || id == "Schema-mysql" || id == "Schema-performance_schema"){
                        return
                    }
                    querySchemaName = id;
                });
                if(querySchemaName != null){
                    showSchemaTableList(querySchemaName);
                }
            }else{
                if( $("#Schema-"+querySchemaName).length <= 0){
                    return false;
                }
                showSchemaTableList("Schema-"+querySchemaName);
                if(queryTableName!=null){
                    GetTableToServerList(querySchemaName,queryTableName);
                }
            }
        }

        init();

        $("#TableSearchName").keyup(function(event){
            if(event.keyCode ==13){
                var TableSearchName = $("#TableSearchName").val();
                $('#TableListContair a').each(function(){
                    var tmpName = $(this).find(":input[name=table_check_name]").val();
                    if( TableSearchName == "" || tmpName.indexOf(TableSearchName) > -1 ){
                        $(this).show();
                    }else{
                        $(this).hide();
                    }
                });
            }
        });

    </script>



<div class="modal inmodal fade" id="addTableDiv" tabindex="-1" role="dialog"  aria-hidden="true">
    <div class="modal-dialog">
        <div class="modal-content">
            <div class="modal-header">
                <button type="button" class="close" data-dismiss="modal"><span aria-hidden="true">&times;</span><span class="sr-only">Close</span></button>
                <h3 class="modal-title">Add New Table</h3>
            </div>
            <div class="modal-body">
                <table width="100%" border="0">
                    <tr>
                        <td align="right" height="50" width="20%">DB : </td>
                        <td style="text-indent:10px" >
                            <input type="text" name="DbName" id="addTableDbName" class="form-control" placeholder="Database" value="mysqlLocalTest" disabled>
                        </td>
                    </tr>

                    <tr>
                        <td align="right" height="50" width="20%">Schema : </td>
                        <td style="text-indent:10px" >
                            <input type="text" name="schema" class="form-control" placeholder="Database" value="" id="addTableSchema" disabled>
                        </td>
                    </tr>

                    <tr>
                        <td align="right" height="50" width="20%">TableName : </td>
                        <td style="text-indent:10px" >
                            <input type="text" name="TableName" class="form-control" placeholder="TableName" value="" id="addTableName" disabled>
                            <input type="hidden" name="TableName_old" class="form-control"  value="" id="addTableNameOld">
                            <div id="FuzzyMatchingTableNames" style="word-wrap: break-word;word-break: normal; width: auto;"></div>
                        </td>
                    </tr>

                    <tr>
                        <td align="right" height="50" width="20%">FuzzyMatching : </td>
                        <td style="text-indent:10px" >
                            <select class="form-control" name="" id="AddTableIsFuzzyMatching" >
                                <option value="0">No</option>
                                <option value="1">Yes</option>
                            </select>
                            <p style="padding-top: 5px">Yes 可以修改表名 带 * 的模糊匹配规则,例如: binlog_field_test_*</p>
                            <p><a href="/docs#FuzzyMatching" target="_blank">模糊匹配文档</a></p>
                        </td>
                    </tr>
                    <tr>
                        <td align="right" height="50" width="20%">IgnoreTables : </td>
                        <td style="text-indent:10px" >
                            <textarea type="text" name="TableName" class="form-control" value="" id="AddTableIgnoreTable" disabled></textarea>
                            <div style="word-wrap: break-word;word-break: normal; width: auto;">
                                <p>模糊匹配的时候，指定哪些表名不行进行匹配，多个用 逗号 隔开</p>
                                <p>只对增量有效,全量任务无效,全量任务,可以自行修改不查询哪些表</p>
                            </div>
                        </td>
                    </tr>
                    <tr>
                        <td align="right" height="50">ChannelKey :  </td>
                        <td style="text-indent:10px">
                            <select class="form-control" name="" id="AddTableChannel" >
                            {{range $k,$v := .ChannelList}}
                                <option value="{{$k}}">{{$v.Name}}</option>
                            {{end}}
                            </select>
                        </td>
                    </tr>
                </table>

            </div>
            <div class="modal-footer">
                <button type="button" class="btn-sm btn-white" data-dismiss="modal">关闭</button>
                <button type="button" class="btn-sm btn-primary" onclick="AddTable(this)">保存</button>
            </div>
        </div>
    </div>
</div>



<div class="modal inmodal fade" id="updateTableDiv" tabindex="-1" role="dialog"  aria-hidden="true">
    <div class="modal-dialog">
        <div class="modal-content">
            <div class="modal-header">
                <button type="button" class="close" data-dismiss="modal"><span aria-hidden="true">&times;</span><span class="sr-only">Close</span></button>
                <h3 class="modal-title">Update Table ignoreTables </h3>
            </div>
            <div class="modal-body">
                <table width="100%" border="0">
                    <tr>
                        <td align="right" height="50" width="20%">DB : </td>
                        <td style="text-indent:10px" >
                            <input type="text" name="DbName" id="updateTableDbName" class="form-control" placeholder="Database" value="mysqlLocalTest" disabled>
                        </td>
                    </tr>

                    <tr>
                        <td align="right" height="50" width="20%">Schema : </td>
                        <td style="text-indent:10px" >
                            <input type="text" name="schema" class="form-control" placeholder="Database" value="" id="updateTableSchema" disabled>
                        </td>
                    </tr>

                    <tr>
                        <td align="right" height="50" width="20%">TableName : </td>
                        <td style="text-indent:10px" >
                            <input type="text" name="TableName" class="form-control" placeholder="TableName" value="" id="updateTableName" disabled>
                        </td>
                    </tr>
                    <tr>
                        <td align="right" height="50" width="20%">IgnoreTables : </td>
                        <td style="text-indent:10px" >
                            <textarea type="text" name="TableName" class="form-control" placeholder="IgnoreTables" value="" id="updateTableIgnoreTable"></textarea>
                            <div style="word-wrap: break-word;word-break: normal; width: auto;">
                                <p>模糊匹配的时候，指定哪些表名不行进行匹配，多个用 逗号 隔开</p>
                                <p>只对增量有效,全量任务无效,全量任务,可以自行修改不查询哪些表</p>
                            </div>
                        </td>
                    </tr>
                </table>

            </div>
            <div class="modal-footer">
                <button type="button" class="btn-sm btn-white" data-dismiss="modal">关闭</button>
                <button type="button" class="btn-sm btn-primary" onclick="UpdateTable(this)">保存</button>
            </div>
        </div>
    </div>
</div>


<script type="text/javascript">

    function showAddTable(TableNames){
        if (TableNames == ""){
            return false;
        }
        var addTableSchemaName = $("#DatabaseListContair a.active").find("h3").text();
        $("#addTableSchema").val(addTableSchemaName);
        var DbName = $("#DbName").val();
        var url = "/channel/list";
        $("#addTableName").val(TableNames);
        $("#addTableNameOld").val(TableNames);
        $("#AddTableIgnoreTable").val("");
        $.get(url,
                {DbName:DbName},
                function(data,status){
                    if( status != 'success' ){
                        alert("reqeust error, reqeust status : "+status);
                        return false;
                    }
                    var html = '';
                    $.each(data,function(index,v){
                        html += '<option value="'+index+'">'+v.Name+'</option>';
                    });
                    $("#AddTableChannel").html(html);
                },
                'json');
        if ( TableNames.charAt(TableNames.length - 1 ) == ";" || TableNames == "AllTables"){
            $("#AddTableIsFuzzyMatching").val("0");
            $("#AddTableIsFuzzyMatching").attr("disabled","disabled");
            $("#addTableName").attr("disabled","disabled");
            $('#AddTableIgnoreTable').removeAttr("disabled");
        }else{
            $('#AddTableIsFuzzyMatching').removeAttr("disabled");
        }
        $("#addTableDiv").modal('show');
    }

    $("#AddTableIsFuzzyMatching").change(
            function () {
                if( $(this).val() == 1 ){
                    $('#addTableName').removeAttr("disabled");
                    $("#AddTableIgnoreTable").removeAttr("disabled");
                }else{
                    $("#addTableName").val($("#addTableNameOld").val());
                    $("#addTableName").attr("disable","disable");
                    $("#AddTableIgnoreTable").attr("disable","disable");
                    $("#AddTableIgnoreTable").val("");
                }
            }
    );

    $("#addTableName").change(
            function () {
                var tableNames = getTablesByLikeName($(this).val());
                $("#FuzzyMatchingTableNames").width($("#AddTableIsFuzzyMatching").width());
                $("#FuzzyMatchingTableNames").text(tableNames);
            }
    );

    function AddTable(obj){
        if($(obj).text()=="正在执行"){
            return false;
        }
        var DbName = $("#DbName").val();
        var TableNames = $("#addTableName").val();
        
        var SchemaName = $("#addTableSchema").val();
        var IgnoreTable = $("#AddTableIgnoreTable").val();
        var channelid = $("#AddTableChannel").val();
        if ( TableNames == "" ){
            return false;
        }
        if ( TableNames == "*" ){
            return false;
        }
        if ( $("#AddTableIsFuzzyMatching").val() == 1 ){
            if (TableNames.indexOf("*") == -1){
                alert("选择了模糊匹配,但 TableName 匹配规则有问题，没有 *，请参考文档后再进行设置 ！");
                return false;
            }
            var tableNames = getTablesByLikeName(TableNames)
            if (tableNames == ""){
                alert("匹配规则 设置有问题，请参考文档后再进行设置 ！");
                return false;
            }
            if( !confirm("请确认是否匹配正确："+tableNames ) ) {
                return false;
            }
        }
        $(obj).text("正在执行");
        var url = "/table/add";
        var arr = TableNames.split(";");
        $("#addTableDiv").modal('hide');
        batchDelOrAddTableResultFun("开始添加",-1);
        for( var i in arr) {
            var TableName = arr[i];
            if ( TableName == "" ){
                continue;
            }
            if ( TableName == "*" ){
                continue;
            }
            var callback = function(data){
                if(data.status){
                    var tableDivId = md5(SchemaName + "_-" + TableName);
                    if (TableName.indexOf("*") != -1){

                        
                        var html = "";
                        var title = "";
                        title = " title='Bind Channel : " + channelid + "' ";
                        html += '<a class="list-group-item"><div class="tableDiv" title="' + TableName + '" id="' + tableDivId + '">';
                        html += '<h5 class="left" ' + title + ' onClick="GetTableToServerList(\'' + SchemaName + '\',\'' + TableName + '\')">' + TableName + '</h5>';
                        html += '<div class="right1">';
                        html += '<div class="button"><button data-toggle="button" class="btn-sm btn-success" type="button" onclick="ShowLikeTableNames(\''+TableName+'\',\''+IgnoreTable+'\')">多表</button></div>';
                        html += '</div>';
                        html += '<div class="right">';
                        html += '<div class="button"><button data-toggle="button" class="btn-sm btn-danger" type="button" onClick="DelTable(\''+TableName+'\')">DEL</button></div>';

                        html += "<div class='check_input'> <input type='checkbox' name='table_check_name' value='"+TableName+"' style='width: 20px; height: 20px;' /></div>";
                        html += '</div>';
                        html +=	'</div></a>';
                        $("#TableListContair").append(html);

                        DataBaseMap.delete(SchemaName);
                        showSchemaTableList("Schema-"+SchemaName);
                    }else{
                        var html = '<button data-toggle="button" class="btn-danger btn-sm" type="button" onClick="DelTable(\''+TableName+'\')">DEL</button>';
                        $("#" + tableDivId + " .right .button").html(html);
                        $("#" + tableDivId + " .right .input input").prop("checked",false);
                    }

                }
                batchDelOrAddTableResultFun(TableName +" ADD Result:"+data.msg,1);
            }
            Ajax("POST",url,{DbName: DbName, SchemaName: SchemaName, TableName: TableName, ChannelId: parseInt(channelid),IgnoreTable:IgnoreTable},callback,false);
        }
        batchDelOrAddTableResultFun("添加完成",-2);
        $(obj).text("提交");
    }


    function showUpdateTable(DbName,SchemaName,TableName,IgnoreTable){
        $("#updateTableDbName").val(DbName);
        $("#updateTableSchema").val(SchemaName);
        $("#updateTableName").val(TableName);
        $("#updateTableIgnoreTable").val(IgnoreTable);
        $("#updateTableDiv").modal('show');
    }

    function UpdateTable() {
        var DbName = $("#updateTableDbName").val();
        var SchemaName = $("#updateTableSchema").val();
        var TableName = $("#updateTableName").val();
        var IgnoreTable = $("#updateTableIgnoreTable").val();
        var url = "/table/update";
        var callback = function(data){
            alert(data.msg);
            DataBaseMap.delete(SchemaName);
            showSchemaTableList("Schema-"+SchemaName);
            $("#updateTableDiv").modal('hide');
        }
        Ajax("POST",url,{DbName: DbName, SchemaName: SchemaName, TableName: TableName, IgnoreTable:IgnoreTable},callback,false);
    }

</script>






<div class="modal inmodal fade" id="addHistoryDiv" tabindex="-1" role="dialog"  aria-hidden="true">
    <div class="modal-dialog">
        <div class="modal-content">
            <div class="modal-header">
                <button type="button" class="close" data-dismiss="modal"><span aria-hidden="true">&times;</span><span class="sr-only">Close</span></button>
                <h3 class="modal-title">Add New History</h3>
            </div>
            <div class="modal-body">
                <table width="100%" border="0">
                    <tr>
                        <td align="right" height="50" width="20%">DB : </td>
                        <td style="text-indent:10px" >
                            <input type="text" name="DbName" id="addHisotryDbName" class="form-control" placeholder="Database" value="mysqlLocalTest" disabled>
                        </td>
                    </tr>

                    <tr>
                        <td align="right" height="50" width="20%">Schema : </td>
                        <td style="text-indent:10px" >
                            <input type="text" name="schema" class="form-control" placeholder="Database" value="" id="addHisotrySchema" disabled>
                        </td>
                    </tr>

                    <tr>
                        <td align="right" height="50" width="20%">TableName : </td>
                        <td style="text-indent:10px" >
                            <textarea type="text" name="TableName" class="form-control" placeholder="TableName" value="" id="addHisotryTableName" disabled></textarea>

                            <input type="hidden" name="TableName_old" class="form-control" placeholder="TableName" value="" id="addHisotryTableNameOld">
                        </td>
                    </tr>

                    <tr>
                        <td align="right" height="50" width="20%">Where : </td>
                        <td style="text-indent:10px; padding-bottom: 5px;" >
                            <textarea type="text" name="where" class="form-control" placeholder="例如: update_time >= '2019-10-10'; 如果有 OR 等条件的话，请用 ( ) 包起来" value="" id="addHisotryWhere"></textarea>
                        </td>

                    </tr>
                    <tr>
                        <td align="right" height="50" width="20%" valign="top">分页方式 : </td>
                        <td style="text-indent:10px; padding-bottom: 5px;" >
                            <select class="form-control" name="LimitOptimize" id="addHisotryLimitOptimize">
                                <option value="1">BETWEEN</option>
                                <option value="0">LIMIT</option>
                            </select>
                            <p>BETWEEN : 主键是自增自段的时候生效</p>
                            <p>LIMIT   ：常规分页读取方式 </p>
                        </td>
                    </tr>

                    <tr>
                        <td align="right" height="50" width="20%">Select ThreadNum : </td>
                        <td style="text-indent:10px;">
                            <input type="text" name="ThreadCount" class="form-control" placeholder="ThreadNum" value="1" id="addHisotryThreadNum">
                            <span>开启多少个连接并发查询</span>

                        </td>
                    </tr>

                    <tr>
                        <td align="right" height="50" width="20%">Sync ThreadNum : </td>
                        <td style="text-indent:10px;">
                            <input type="text" name="SyncThreadNum" class="form-control" placeholder="SyncThreadNum" value="1" id="addHisotrySyncThreadNum">
                            <span>开启多少个同步协程</span>

                        </td>
                    </tr>

                    <tr>
                        <td align="right" height="50" width="20%">ThreadCountPer : </td>
                        <td style="text-indent:10px" >
                            <input type="text" name="ThreadCountPer" class="form-control" placeholder="ThreadCountPer" value="1000" id="addHisotryThreadCountPer">
                            <span>每次查询读取多少条数据</span>
                        </td>
                    </tr>
                    <tr>
                        <td align="right" height="50">ToServer :  </td>
                        <td style="text-indent:10px" id="addHisotryToServer">

                        </td>
                    </tr>
                </table>

            </div>
            <div class="modal-footer">
                <button type="button" class="btn-sm btn-white" data-dismiss="modal">关闭</button>
                <button type="button" class="btn-sm btn-primary" id="AddHistoryBtn">保存</button>
            </div>
        </div>
    </div>
</div>


<script type="text/javascript">

    $("#historyAddBtn").click(
            function () {
                var url = "/table/toserver/list";
                var DbName = $("#DbName").val();
                var SchemaName = $("#tableToServerListContair").attr("schema");
                var TableName = $("#tableToServerListContair").attr("TableName");
                if( SchemaName == "AllDataBases" ){
                    alert("当前不支持配置 全量所有库表 ！");
                    return;
                }
                var callback = function (data) {
                    var Html = "";
                    $.each(data,function(index,v){
                        Html += "<p><input style='height: 20px; width: 20px;' type='checkbox' name='addHistoryToServerID' value='"+ v.ToServerID + "' />"+v.PluginName +"-"+v.ToServerKey+"</p>";
                    });

                    $("#addHisotryToServer").html(Html);
                };
                var ajaxParam =  {DbName:DbName,SchemaName:SchemaName,TableName:TableName};
                Ajax("GET",url, ajaxParam,callback,true);

                var TableNames = "";

                if ( TableName == "AllTables" || TableName.indexOf("*") != -1 ){
                    TableNames = getTablesByLikeName(TableName);
                    if ( TableNames == "" ) {
                        alert("没有匹配到表名！请检查配置是否正确，或者创建一个符合当前规则表，再进行选择配置！");
                        return false;
                    }
                }else{
                    TableNames = TableName
                }

                $("#addHisotrySchema").val(SchemaName);
                $("#addHisotryTableName").val(TableNames);
                if( TableName != TableNames ){
                    $("#addHisotryTableName").removeAttr("disabled");
                }
                $("#addHisotryTableNameOld").val(TableName);
                $("#addHistoryDiv").modal('show');
            }
    );

    $("#AddHistoryBtn").click(
            function () {
                var ThreadNum       = $("#addHisotryThreadNum").val();
                var ThreadCountPer  = $("#addHisotryThreadCountPer").val();
                var DbName          = $("#addHisotryDbName").val();
                var SchemaName     = $("#addHisotrySchema").val();
                var TableNames     = $("#addHisotryTableName").val();
                var TableName      = $("#addHisotryTableNameOld").val();
                var where           = $("#addHisotryWhere").val();
                var LimitOptimize   = $("#addHisotryLimitOptimize").val();
                var SyncThreadNum   = $("#addHisotrySyncThreadNum").val();

                var ToServerIds = [];
                $.each($("#addHisotryToServer input:checkbox:checked"),function(){
                    ToServerIds.push(parseInt($(this).val()));
                });
                if (ToServerIds.length == 0){
                    alert("请选择 ToServer");
                    return false;
                }
                if (isNaN(ThreadNum)){
                    alert("ThreadNum must be int!");
                    return false;
                }
                if (isNaN(SyncThreadNum) || ( parseInt(SyncThreadNum) * ToServerIds.length > 16384 ) ){
                    alert("SyncThreadNum must be int,and SyncThreadNum * ToServerIds.length <= 16384 ! ");
                    return false;
                }
                if (isNaN(ThreadCountPer)){
                    alert("ThreadCountPer must be int!");
                    return false;
                }
                if ( TableNames == "" ){
                    alert("没有匹配到数据表!");
                    return false;
                }
                if ( TableName != TableNames ){
                    var reg;
                    if (TableName == "AllTables") {
                        reg = new RegExp(".*");
                    }else{
                        reg = new RegExp(TransferLikeTableReq(TableName));
                    }
                    var arr = TableNames.split(";");
                    for( var i in arr) {
                        var tmp_name = arr[i];
                        if (tmp_name == ""){
                            continue;
                        }
                        if ( tmp_name.indexOf("*") != -1 ){
                            alert(tmp_name +" 被匹配的表中不能包含 * ");
                            return;
                        }
                        var regResult = tmp_name.match(reg);
                        if ( regResult == null || regResult == undefined){
                            alert(tmp_name +" 不符合："+TableName + " 匹配规则！");
                            return;
                        }
                    }
                }

                var Property = {};
                Property["ThreadNum"]       = parseInt(ThreadNum);
                Property["ThreadCountPer"]  = parseInt(ThreadCountPer);
                Property["Where"]           = $.trim(where);
                Property["LimitOptimize"]   = parseInt(LimitOptimize);
                Property["SyncThreadNum"]   = parseInt(SyncThreadNum);

                var url = "/history/add";

                var callback = function (data) {
                    if(data.status) {
                        alert("success,请点击开始按钮进行开启 初始化操作")
                        window.location.href = "/history/index?DbName=" + DbName + "&SchemaName=" + SchemaName + "&TableName=" + TableName;
                    }else{
                        alert(data.msg);
                    }
                };
                var ajaxParam =  {DbName:DbName,SchemaName:SchemaName,TableName:TableName,TableNames:TableNames,property:Property,ToserverIds:ToServerIds};
                Ajax("POST",url, ajaxParam,callback,true);
            }
    );

    $("#addHisotryWhere").change(
            function () {
                if ( $(this).val() == "" ) {
                    $("#addHisotryLimitOptimize").val(1);
                }else{
                    $("#addHisotryLimitOptimize").val(0);
                }
            }
    );


</script>





<div class="footer">
    <div class="pull-right">
    </div>
    <div>
        <strong>Copyright</strong> <a href="http://www.xbifrost.com" target="_blank">xBifrost.com</a> &copy; 2020
        &nbsp;&nbsp;&nbsp;&nbsp;
        By：<a href="http://www.xbifrost.com" target="_blank">jc3wish </a>  version: <span id="version_contair">v1.6.0-beta.03</span>

    </div>
</div>

</body>
</html>
<script type="text/javascript">
    $(function(){
        $(":text").change(
                function(){
                    $(this).val($.trim($(this).val()));
                }
        );
    });
</script>
<link href="/js/plugins/chardin.js/chardinjs.css" rel="stylesheet">
<script src="/js/plugins/chardin.js/chardinjs.js?v0.2.0"></script>
<script src="/js/guide.js?v1.2.1_20200516"></script>

`
