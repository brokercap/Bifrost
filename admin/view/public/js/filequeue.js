function fileQueueStart(thisObj,DbName,SchemaName,TableName,ToServerId,Index){
    if ( !confirm("非极端情况下,不要手工点击启动文件队列，进行启动! \n确定 需要 继续 开启吗？！！") ){
        return false;
    }
    var url = "/table/toserver/filequeue/update";
    var callback = function (data) {
        alert(data.msg)
        if(data.status) {
            $(thisObj).remove();
        }
    };
    Ajax("POST",url, {DbName:DbName,SchemaName:SchemaName,TableName:TableName,ToServerId:ToServerId,Index:Index},callback,true);
}

function getFileQueueInfo(thisObj,DbName,SchemaName,TableName,ToServerId,Index){
    var url = "/table/toserver/filequeue/getinfo";
    var callback = function (data) {
        if(!data.status) {
            alert(data.msg);
            return ;
        }
        var html = "<p>文件队列信息：</p>";
        html += "<p>Path:"+data.data.Path+"</p>";
        html += "<p>最小文件:"+data.data.MinId+"</p>";
        html += "<p>最大文件:"+data.data.MaxId+"</p>";
        html += "<p>文件总数:"+data.data.FileCount+"</p>";

        var UnackFileList = data.data.UnackFileList;
        if ( UnackFileList.length > 0 ) {
            html += "<p>Uack 文件信息：</p>";
            html += "<table class=\"table\">"
            html += "<tr><td>文件名</td><td title='未被ack的数量'>Unack</td><td title='是否整个文件已加载到内存'>AllInMemory</td><td title='从磁盘中读取出来的数量'>TotalCount</td></tr>"
            for (var j = 0, len = UnackFileList.length; j < len; j++) {
                html += "<tr>";
                html += "<td>"+ UnackFileList[j].Id+"</td>";
                html += "<td>"+ UnackFileList[j].UnackCount+"</td>";
                html += "<td>"+ UnackFileList[j].AllInMemory+"</td>";
                html += "<td>"+ UnackFileList[j].TotalCount+"</td>";
                html += "</tr>";
                //html += "<p>文件名:" + UnackFileList[j].Id + " <span title='未被ack的数量'>Unack:</span>" + UnackFileList[j].UnackCount + "  <span title='是否整个文件已加载到内存'>AllInMemory:</span> " + UnackFileList[j].AllInMemory + " <span title='从磁盘中读取出来的数量'>TotalCount:</span> " + UnackFileList[j].TotalCount + "</p>";
            }
            html += "</table>"
        }
        $(thisObj).parent().parent().find(".fileInfoDiv").html(html);
    };
    Ajax("GET",url, {DbName:DbName,SchemaName:SchemaName,TableName:TableName,ToServerId:ToServerId,Index:Index},callback,true);
}