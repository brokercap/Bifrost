function fileQueueStart(thisObj,dbname,schema_name,table_name,to_server_id,index){
    var url = "/table/toserver/filequeue/update";
    $.post(url,
        {dbname:dbname,schema_name:schema_name,table_name:table_name,to_server_id:to_server_id,index:index},
        function(data,status){
            if( status != 'success' ){
                alert("reqeust error, reqeust status : "+status);
                return false;
            }
            alert(data.msg)
            if(data.status) {
                $(thisObj).remove();
            }
        },
        'json');
}

function getFileQueueInfo(thisObj,dbname,schema_name,table_name,to_server_id,index){
    var url = "/table/toserver/filequeue/getinfo";
    $.post(url,
        {dbname:dbname,schema_name:schema_name,table_name:table_name,to_server_id:to_server_id,index:index},
        function(data,status){
            if( status != 'success' ){
                alert("reqeust error, reqeust status : "+status);
                return false;
            }
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
            var htmlId = dbname+"_"+schema_name+"_"+table_name+"_"+to_server_id;
            $("#"+htmlId).html(html);
        },
        'json');
}