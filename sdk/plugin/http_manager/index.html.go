package http_manager

var IndexHtml = `
<!DOCTYPE html>
<html>

<head>

    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>mysqlTest - Detail - Bifrost</title>
    <link rel="shortcut icon" href="favicon.ico">
    <link href="/css/bootstrap.min14ed.css?v=3.3.6" rel="stylesheet">
    <link href="/css/style.min862f.css?v=4.1.0" rel="stylesheet">
    <script src="/js/jquery.min.js?v=2.1.4"></script>
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
                            <a  href="/db/list" class="dropdown-toggle" > 数据源 </a>
                        </li>
                        <li class="dropdown">
                            <a href="/toserver/list" class="dropdown-toggle"> 目标库列表 </a>
                        </li>
                        <li class="dropdown">
                            <a  href="/flow/index" class="dropdown-toggle"> 流量 </a>
                        </li>

                        <li class="dropdown">
                            <a  href="/synclist" class="dropdown-toggle"> 表同步列表 </a>
                        </li>

                        <li class="dropdown">
                            <a  href="/history/list" class="dropdown-toggle"> 全量任务 </a>
                        </li>
                        <li class="dropdown">
                            <a href="/plugin/list" class="dropdown-toggle"> 插件 </a>
                        </li>
                        <li class="dropdown">
                            <a href="/warning/config/list" class="dropdown-toggle"> 报警 </a>
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
    <input type="hidden" value="mysqlTest" id="dbname" />
    <div class="row">
        <div class="col-sm-2" id="MyWebLeft_1">

            <div class="ibox float-e-margins">
                <div class="ibox-title">
                    <h5>mysqlTest - Schema List</h5>
                </div>
                <div class="ibox-content">
                    <div class="list-group" id="DatabaseListContair">
                    
                        <a class="list-group-item" id="Schema-information_schema">
                            <h3 class="list-group-item-heading">information_schema</h3>
                        </a>
                    
                        <a class="list-group-item" id="Schema-bifrost_test">
                            <h3 class="list-group-item-heading">bifrost_test</h3>
                        </a>
                    
                        <a class="list-group-item" id="Schema-mysql">
                            <h3 class="list-group-item-heading">mysql</h3>
                        </a>
                    
                        <a class="list-group-item" id="Schema-performance_schema">
                            <h3 class="list-group-item-heading">performance_schema</h3>
                        </a>
                    
                    </div>

                </div>
            </div>

        </div>
        <div class="col-sm-3" style=" padding-left: 0px;" id="MyWebLeft_2">
            <div class="ibox float-e-margins">
                <div class="ibox-title">
                    <h5>Table List</h5>
                    <div id="MyWebHideBtn" onclick="showOrHideMyWeb();" style="float:right; font-size:14px; font-weight:600; color:#666; cursor:pointer">Hide</div>
                </div>
                <style type="text/css">
                    .tableDiv{display:block; width:100%; height: 25px; position: relative}
                    .tableDiv .left{ display:block; float:left; width:79%; line-height:100%;word-wrap: break-word;  }
                    .tableDiv .right{ display:block; position: absolute; right: -10px ;top: 0px; width:78px; line-height: 25px;}
                    .tableDiv .right1{ display:block; position: absolute; right: 48px ;top: 0px; width:78px; line-height: 25px;}
                    .tableDiv .right .button{ float: left;}
                    .tableDiv .right .check_input{ float: left;margin-left: 8px; margin-top: 2px}
                    .tableDiv .right1 .button{ float: left;}
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
                            <table id="tableToServerListContair" schema="" table_name="" dbname="" data-toggle="table" data-query-params="queryParams" data-mobile-responsive="true" data-height="auto" data-pagination="false" data-icon-size="outline">
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
                                    
                                        <option value="blackholeTest" pluginName="blackhole" pluginVersion="v1.1.0">blackhole -- blackholeTest</option>
                                    
                                        <option value="clickHouseTest" pluginName="clickhouse" pluginVersion="v1.1.0-rc.02">clickhouse -- clickHouseTest</option>
                                    
                                        <option value="redisTest" pluginName="redis" pluginVersion="v1.1.0">redis -- redisTest</option>
                                    
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
                                <input type="text" name="dbname" id="addTableDbName" class="form-control" placeholder="Database" value="mysqlTest" disabled>
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
                                <input type="text" name="table_name" class="form-control" placeholder="TableName" value="" id="addTableName" disabled>
                            </td>
                        </tr>
                        <tr>
                            <td align="right" height="50">ChannelKey :  </td>
                            <td style="text-indent:10px">
                                <select class="form-control" name="" id="AddTableChannel" >
                                
                                    <option value="1">default</option>
                                
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
                                <input type="text" name="dbname" id="addHisotryDbName" class="form-control" placeholder="Database" value="mysqlTest" disabled>
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
                                <input type="text" name="table_name" class="form-control" placeholder="TableName" value="" id="addHisotryTableName" disabled>
                            </td>
                        </tr>


                        <tr>
                            <td align="right" height="50" width="20%">ThreadNum : </td>
                            <td style="text-indent:10px" >
                                <input type="text" name="ThreadCount" class="form-control" placeholder="ThreadNum" value="1" id="addHisotryThreadNum">
                                <span>开启多少个连接并发查询</span>
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
    

    <script src="/js/bootstrap.min.js?v=3.3.6"></script>
    <script src="/js/plugins/bootstrap-table/bootstrap-table.min.js"></script>
    <script type="text/javascript">

        var dbname = "mysqlTest";
        var OnFoucsInputId = "";
        var tableDataTypeMap = {};

        function getTableFieldType(field) {
            return tableDataTypeMap[field];
        }

        function clearTableDataMap() {
            tableDataTypeMap = {};
        }
        function setTableFieldType(field,dataType) {
            tableDataTypeMap[field] = dataType;
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

        function showAddTable(table_names){
            if (table_names == ""){
                return false;
            }
            var addTableSchemaName = $("#DatabaseListContair a.active").find("h3").text();
            $("#addTableSchema").val(addTableSchemaName);
            var dbname = $("#dbname").val();
            var url = "/channel/list";
            $("#addTableName").val(table_names);
            $.get(url,
                    {dbname:dbname},
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
            $("#addTableDiv").modal('show');
        }

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
                var table_names = "";
                $("#TableListContair .check_input input[type='checkbox']:checked").each(function (index, item) {
                    if($(this).parent().parent().find(".button button").text()=="ADD"){
                        table_names += $(this).val()+";"
                    }
                });
                showAddTable(table_names);
            }
        );

        $("#batchTableDeleteBtn").click(
                function () {
                    var table_names = "";
                    $("#TableListContair .check_input input[type='checkbox']:checked").each(function (index, item) {
                        if($(this).parent().parent().find(".button button").text()=="DEL"){
                            table_names += $(this).val()+";"
                        }
                    });
                    DelTable(table_names);
                }
        );

        function AddTable(obj){
            if($(obj).text()=="正在执行"){
                return false;
            }
            var dbname = $("#dbname").val();
            var table_names = $("#addTableName").val();
            var schema_name = $("#addTableSchema").val();
            var channelid = $("#AddTableChannel").val();
            if ( table_names == "" ){
                return false;
            }
            $(obj).text("正在执行");
            var url = "/table/add";
            var arr = table_names.split(";");
            $("#addTableDiv").modal('hide');
            batchDelOrAddTableResultFun("开始添加",-1);
            for( var i in arr) {
                var table_name = arr[i];
                if ( table_name == "" ){
                    continue;
                }
                $.ajax({
                    type : "post",
                    url : url,
                    data:{dbname: dbname, schema_name: schema_name, table_name: table_name, channelid: channelid},
                    async : false,  
                    dataType:"json",
                    success : function(data,status){
                        if( status != 'success' ){
                            alert("reqeust error, reqeust status : "+status);
                            return false;
                        }
                        if(data.status){
                            html = '<button data-toggle="button" class="btn-danger btn-sm" type="button" onClick="DelTable(\''+table_name+'\')">DEL</button>';
                            $("#" + schema_name + "_-" + table_name + " .right .button").html(html);
                            $("#" + schema_name + "_-" + table_name + " .right .input input").prop("checked",false);
                        }
                        batchDelOrAddTableResultFun(table_name +" ADD Result:"+data.msg,1);
                    }
                });
            }
            batchDelOrAddTableResultFun("添加完成",-2);
            $(obj).text("提交");
        }

        function DelTable(table_names){
            if(table_names == ""){
                return false;
            }
            if (!confirm("确定删除 [ "+table_names+" ] ?删除后不能恢复!!!")){
                return false;
            }
            var dbname = $("#dbname").val();
            var schema_name = $("#DatabaseListContair a.active").find("h3").text();
            var url = "/table/del";
            var arr = table_names.split(";");
            batchDelOrAddTableResultFun("开始删除",-1);
            for( var i in arr) {
                var table_name = arr[i];
                if ( table_name == "" ){
                    continue;
                }
                $.ajax({
                    type: "post",
                    url: url,
                    data: {dbname: dbname, schema_name: schema_name, table_name: table_name},
                    async: false,  
                    dataType: "json",
                    success: function (data, status) {
                        if (status != 'success') {
                            alert("reqeust error, reqeust status : " + status);
                            return false;
                        }
                        if (data.status) {
                            html = '<button data-toggle="button" class="btn-warning btn-sm" type="button" onClick="showAddTable(\'' + table_name + '\')">ADD</button>';
                            $("#" + schema_name + "_-" + table_name + " .right .button").html(html);
                        }
                        batchDelOrAddTableResultFun(table_name +" DEL Result:"+data.msg,1);
                    }
                });
            }
            batchDelOrAddTableResultFun("删除完成",-2);
        }
        function ChangeTableFlowBtnHref(schema_name,table_name){
            $("#tableFlowBtn").attr("href","/flow/index?dbname="+dbname+"&schema="+schema_name+"&table_name="+table_name);
            $("#tableHistoryListBtn").attr("href","/history/list?dbname="+dbname+"&schema="+schema_name+"&table_name="+table_name);
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

        function GetTableToServerList(schema_name,table_name){
            var key = schema_name+"_-"+table_name;
            if ($("#"+key+" .right button").text() == "ADD"){
                table_check_input(key);
                return  false;
            }
            $("#TableListContair a").removeClass("active");
            $("#"+key).parent("a").addClass("active");
            ChangeTableFlowBtnHref(schema_name,table_name);
            var url = "/table/toserverlist"
            $.post(url,
                    {dbname:dbname,schema_name:schema_name,table_name:table_name},
                    function(data,status){
                        if( status != 'success' ){
                            alert("reqeust error, reqeust status : "+status);
                            return false;
                        }
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
                                ErrorHtml = "<p>Err:"+v.Error+"</p><p>Data:"+v.ErrorWaitData+"</p><p><button data-toggle='button' class='btn-sm btn-primary' onClick='DealWaitErr(this,"+v.ToServerID+")' type='button'>Miss</button></p>";
                            }
                            var op = "";
                            if(v.Status == "running" || v.Status == ""){
                                op = '<button data-toggle="button" class="btn-sm btn-danger btn-sm" type="button" onClick="DelTableToServer(this,'+v.ToServerID+')">DEL</button>';
                            }else{
                                op = v.Status;
                            }
                            var others = "";

                            others += "<p>MustBeSuccess: "+v.MustBeSuccess+"</p><p>FilterQuery: "+v.FilterQuery+"</p><p>FilterUpdate: "+v.FilterUpdate+"</p>";
                            others += "<p title=\"最后一个成功处理的位点\">BinlogFileNum: "+v.BinlogFileNum+"</p><p>BinlogPos: "+v.BinlogPosition+"</p>";
                            others += "<p>LastBinlogFileNum: "+v.LastBinlogFileNum+"</p>";
                            others += "<p title=\"队列最后一个位点\">LastBinlogPosition: "+v.LastBinlogPosition+"</p>";
                            if ("QueueMsgCount" in v) {
                                others += "<p title=\"已解析出来的数据,有多少条消息堆积在队列中待同步\">QueueMsgCount: " + v.QueueMsgCount + "</p>";
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
                        $("#tableToServerListContair").attr("dbname",dbname);
                        $("#tableToServerListContair").attr("schema",schema_name);
                        $("#tableToServerListContair").attr("table_name",table_name);
                        $("#tableToServerListContair").bootstrapTable("load",e);
                        GetTableFields(schema_name,table_name);
                    },
                    'json');
        }

        function DelTableToServer(obj,ToServerID){
            var index = $(obj).parent().parent().children().eq(0).text();
            if (!confirm("确定删除第 [ "+ index +" ] 条记录？删除将不能恢复？！！！")){
                return false;
            }
            var dbname = $("#tableToServerListContair").attr("dbname");
            var schema_name = $("#tableToServerListContair").attr("schema");
            var table_name = $("#tableToServerListContair").attr("table_name");
            var url = "/table/deltoserver"
            $.post(url,
                    {dbname:dbname,schema_name:schema_name,table_name:table_name,index:index,to_server_id:ToServerID},
                    function(data,status){
                        if( status != 'success' ){
                            alert("reqeust error, reqeust status : "+status);
                            return false;
                        }
                        if (!data.status){
                            alert(data.msg);
                            return;
                        }
                        $(obj).parent().parent('tr').remove();
                        GetTableToServerList(schema_name,table_name);
                    },
                    'json');
        }
        var TableMap = new Map();
        function GetTableFields(schema_name,table_name){
            var key = schema_name+"_-"+table_name;
            if (!TableMap.has(key) || TableMap.get(key) == undefined){
                var url = '/db/tablefields';
                $.post(url,
                        {dbname:dbname,schema_name:schema_name,table_name:table_name},
                        function(data,status){
                            if( status != 'success' ){
                                alert("reqeust error, reqeust status : "+status);
                                return false;
                            }
                            TableMap.set(key,data);
                            showFieldsList(key);
                        },
                        'json');
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
        var DataBaseMap = new Map();

        function showSchemaTableList(id){
            $("#DatabaseListContair a").removeClass("active");
            $("#"+id).addClass("active");
            var schema_name = $("#"+id).find("h3").text();
            var dbname = $("#dbname").val();
            var url = "/db/tablelist";
            var showTableList = function(data){
                $("#TableListContair").html("");
                $.each(data,function(index,v) {
                    var html = "";
                    var title = "";
                    if (v.ChannelName != "") {
                        title = " title='Bind Channel : " + v.ChannelName + "' ";
                    }
                    html += '<a class="list-group-item"><div class="tableDiv" title="' + v.TableName + '" id="' + schema_name + '_-' + v.TableName + '">';
                    html += '<h5 class="left" ' + title + ' onClick="GetTableToServerList(\'' + schema_name + '\',\'' + v.TableName + '\')">' + v.TableName + '</h5>';
                    if (v.TableType.toUpperCase().indexOf("VIEW") != -1) {
                        html += '<div class="right1">';
                        html += '<div class="button"><button data-toggle="button" class="btn-sm btn-success" type="button">视图</button></div>';
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
            if (!DataBaseMap.has(schema_name) || DataBaseMap.get(schema_name) == undefined){
                $.ajax({
                    type : "get",
                    url : url,
                    data:{dbname:dbname,schema_name:schema_name},
                    async : false,  
                    dataType:"json",
                    success : function(data,status){
                        if( status != 'success' ){
                            alert("reqeust error, reqeust status : "+status);
                            return false;
                        }
                        DataBaseMap.set(schema_name,data);
                        showTableList(data);
                    }
                });
            }else{
                showTableList(DataBaseMap.get(schema_name));
            }
            showBatchDelOrAddBtn();
        }
        $(function(){
            $("#plugin_param_div").on("click",":text,textarea",function(){
                OnFoucsInputId = $(this).attr("id");
            });

            $("#DatabaseListContair a").click(
                    function(){
                        showSchemaTableList($(this).attr("id"));
                    }
            );
            var doChangeToServer = function(){
                if($("#addToServerKey").val()==null){
                    if(confirm("目标库地址为空，是否跳转到 添加 目标库 ？")){
                        window.location.href = "/toserver/list";
                        return;
                    }else{
                        return;
                    }
                }
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
                        var p = doGetPluginParam();
                        if (p.status == false){
                            alert(p.msg);
                            return false;
                        }
                        var dbname = $("#tableToServerListContair").attr("dbname");
                        var schema_name = $("#tableToServerListContair").attr("schema");
                        var table_name = $("#tableToServerListContair").attr("table_name");
                        if (table_name == ""){
                            alert("selected table please!");
                            return false;
                        }
                        var MustBeSuccess = $("#MustBeSuccess").val();
                        var addToServerKey = $("#addToServerKey").val();
                        var fieldlist = [];
                        $.each($("#TableFieldsContair input:checkbox:checked"),function(){
                            fieldlist.push($(this).val());
                        });
						var FilterQuery = $("#FilterQuery").val();
						var FilterUpdate = $("#FilterUpdate").val();
                        var pluginName = $("#addToServerKey").find("option:selected").attr("pluginName");
                        var url = '/table/addtoserver';
                        var data = {
                            dbname:dbname,
                            schema_name:schema_name,
                            table_name:table_name,
                            toserver_key:addToServerKey,
                            plugin_name:pluginName,
                            mustbe:MustBeSuccess,
							FilterQuery:FilterQuery,
							FilterUpdate:FilterUpdate,
                            fieldlist:fieldlist.toString(),
                            param:JSON.stringify(p.data),
                        }
                        $.post(url,
                                data,
                                function(data,status){
                                    if( status != 'success' ){
                                        alert("reqeust error, reqeust status : "+status);
                                        return false;
                                    }
                                    if(!data.status){
                                        alert(data.msg);
                                        return false;
                                    }
                                    GetTableToServerList(schema_name,table_name);
									doChangeToServer();
                                },
                                'json');
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
                        var p = doGetPluginParam();
                        if (p.status == false){
                            alert(p.msg);
                            return false;
                        }
                        var dbname = $("#tableToServerListContair").attr("dbname");
                        var schema_name = $("#tableToServerListContair").attr("schema");
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
                        var addToServerKey = $("#addToServerKey").val();

                        var FilterQuery = $("#FilterQuery").val();
                        var FilterUpdate = $("#FilterUpdate").val();
                        var pluginName = $("#addToServerKey").find("option:selected").attr("pluginName");
                        var url = '/table/addtoserver';
                        var data = {
                            dbname: dbname,
                            schema_name: schema_name,
                            table_name: "",
                            toserver_key: addToServerKey,
                            plugin_name: pluginName,
                            mustbe: MustBeSuccess,
                            FilterQuery: FilterQuery,
                            FilterUpdate: FilterUpdate,
                            fieldlist: fieldlist.toString(),
                            param: JSON.stringify(p.data),
                        }
                        $(obj).text("正在提交");
                        $("#batchAddToServerResultDiv").html("");
                        var resultFun = function (content) {
                            $("#batchAddToServerResultDiv").append("<p>"+content+"</p>");
                        }
                        resultFun("开始提交");
                        for(var i in tableArr) {
                            data.table_name = tableArr[i];
                            $.ajax({
                                type : "post",
                                url : url,
                                data:data,
                                async : false,  
                                dataType:"json",
                                success : function(r,status){
                                    if( status != 'success' ){
                                        resultFun(tableArr[i]+ " reqeust error, reqeust status : "+status);
                                        return false;
                                    }
                                    if(r.status){
                                        resultFun(tableArr[i]+" success");
                                    }else{
                                        resultFun(tableArr[i]+ " "+ r.msg);
                                    }
                                }
                            });
                        }
                        resultFun("执行完成");
                        $(obj).text("批量提交");
                    }
            );

        });

        function DealWaitErr(obj,ToServerID){
            var thisButton = $(obj);
            var index0 = $(obj).parent().parent().parent("tr").children().eq(0).html();
            var dbname = $("#tableToServerListContair").attr("dbname");
            var schema_name = $("#tableToServerListContair").attr("schema");
            var table_name = $("#tableToServerListContair").attr("table_name");
            var index = index0.split("/")[0];
            var url = "/table/toserver/deal";
            $.post(
                    url,
                    {dbname: dbname,schema_name:schema_name,table_name:table_name,to_server_id:ToServerID,index:index},
                    function(data,status){
                        if( status != 'success' ){
                            alert("reqeust error, reqeust status : "+status);
                            return false;
                        }
                        if(!data.status){
                            alert(data.msg);
                            return false;
                        }
                        $(thisButton).parent().parent().html("");
                    },
                    'json'
            );
        }


        function getQueryString(name) {
            var reg = new RegExp("(^|&)" + name + "=([^&]*)(&|$)", "i");
            var r = window.location.search.substr(1).match(reg);
            if (r != null) return unescape(r[2]);
            return null;
        }

        function init(){
            var querySchemaName = getQueryString("schema");
            var queryTableName = getQueryString("table_name");
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


        $("#historyAddBtn").click(
            function () {
                var url = "/table/toserverlist";
                var dbname = $("#dbname").val();
                var schema_name = $("#tableToServerListContair").attr("schema");
                var table_name = $("#tableToServerListContair").attr("table_name");
                $.post(url,
                        {dbname:dbname,schema_name:schema_name,table_name:table_name},
                        function(data,status){
                            if( status != 'success' ){
                                alert("reqeust error, reqeust status : "+status);
                                return false;
                            }
                            var Html = "";
                            $.each(data,function(index,v){
                                Html += "<p><input style='height: 20px; width: 20px;' type='checkbox' name='addHistoryToServerID' value='"+ v.ToServerID + "' />"+v.PluginName +"-"+v.ToServerKey+"</p>";
                            });

                            $("#addHisotryToServer").html(Html);
                        },
                        'json');

                $("#addHisotrySchema").val(schema_name);
                $("#addHisotryTableName").val(table_name);
                $("#addHistoryDiv").modal('show');
            }
        );
        
        $("#AddHistoryBtn").click(
            function () {
                var ThreadNum       = $("#addHisotryThreadNum").val();
                var ThreadCountPer  = $("#addHisotryThreadCountPer").val();
                var dbname          = $("#addHisotryDbName").val();
                var schema_name     = $("#addHisotrySchema").val();
                var table_name      = $("#addHisotryTableName").val();

                var ToServerIds = [];
                $.each($("#addHisotryToServer input:checkbox:checked"),function(){
                    ToServerIds.push(parseInt($(this).val()));
                });
                if (isNaN(ThreadNum)){
                    alert("ThreadNum must be int!");
                    return false;
                }
                if (isNaN(ThreadCountPer)){
                    alert("ThreadCountPer must be int!");
                    return false;
                }
                if (ToServerIds.length == 0){
                    alert("请选择 ToServer");
                    return false;
                }
                var Property = {};
                Property["ThreadNum"]       = parseInt(ThreadNum);
                Property["ThreadCountPer"]  = parseInt(ThreadCountPer);

                var url = "/history/add";
                $.post(url,
                        {dbname:dbname,schema_name:schema_name,table_name:table_name,property:JSON.stringify(Property),ToserverIds:JSON.stringify(ToServerIds)},
                        function(data,status){
                            if( status != 'success' ){
                                alert("reqeust error, reqeust status : "+status);
                                return false;
                            }
                            if(data.status) {
                                alert("success,请点击开始按钮进行开启 初始化操作")
                                window.location.href = "/history/list?dbname=" + dbname + "&schema_name=" + schema_name + "&table_name=" + table_name;
                            }else{
                                alert(data.msg);
                            }
                        },
                        'json');

            }
        );

    </script>


<div class="footer">
    <div class="pull-right">
    </div>
    <div>
        <strong>Copyright</strong> <a href="http://www.xbifrost.com" target="_blank">xBifrost.com</a> &copy; 2020
        &nbsp;&nbsp;&nbsp;&nbsp;
        By：<a href="http://www.xbifrost.com" target="_blank">jc3wish </a>  version: <span id="version_contair"></span>

    </div>
</div>

</body>
</html>
<script type="text/javascript">
    $(function(){
        $.get(
            "/getversion",
            {},
            function (data, status) {
                if (status != 'success') {
                    
                    return;
                }
                if (!data.status) {
                    
                    return;
                } else {
                    $("#version_contair").text(data.msg);
                }
            },
            'json');
        $(":text").change(
			function(){
				$(this).val($.trim($(this).val()));
			}
		);
    });
</script>
`
