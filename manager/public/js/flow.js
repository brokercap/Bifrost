class FlowClass {
    CountType = "条";
    ByteSizeType = "b";
    dbName  = "";
    schema = "";
    tableName = "";
    ChanneId = "";
    type = "tenminute";
    CanvasId = "";
    ChartType = "line";
    DisplayFormat = "increment"; //increment,full;

    constructor(Type,CanvasId,DisplayFormat) {
        this.type = Type;
        this.CanvasId = CanvasId;
        this.DisplayFormat = DisplayFormat;
    }
    setDbName=function(dbName){
        this.dbName = dbName;
    }
    setSchema=function(schema){
        this.schema = schema;
    }

    setTableName=function(tableName){
        this.tableName = tableName;
    }

    setType=function(type){
        this.type = type;
    }
    setCanvasId=function(CanvasId){
        this.CanvasId = CanvasId;
    }

    setDisplayFormat=function(DisplayFormat){
        this.DisplayFormat = DisplayFormat;
    }

    add0 = function(m){return m<10?'0'+m:m }

    TimeFormat = function(timeUnix)
    {
        var time = new Date(parseInt(timeUnix)*1000);
        var y = time.getFullYear();
        var m = time.getMonth();
        var d = time.getDate();
        var h = time.getHours();
        var mm = time.getMinutes();
        var s = time.getSeconds();
        return y+'-'+this.add0(m)+'-'+this.add0(d)+' '+this.add0(h)+':'+this.add0(mm)+':'+this.add0(s);
    }

    rewrite_data = function (d){
        if (d.length == 0){
            return false
        }
        var ChartData = {};
        ChartData.options = {};
        ChartData.labels=[];
        ChartData.datasets=[];

        var ByteSizeData={};
        ByteSizeData.data=[];
        ByteSizeData.fillColor= "#1ab394";
        ByteSizeData.strokeColor = "#1ab394";
        ByteSizeData.highlightFill="#1ab394";
        ByteSizeData.highlightStroke= "#1ab394";

        ByteSizeData.borderColor ="#1ab394";
        ByteSizeData.label = "ByteSize("+ByteSizeType+")";

        var CountData = {};
        CountData.data=[];
        CountData.fillColor= "#5CACEE";
        CountData.strokeColor = "#5CACEE";
        CountData.highlightFill="#5CACEE";
        CountData.highlightStroke= "#5CACEE";

        CountData.borderColor ="#5CACEE";
        CountData.label = "Count("+CountType+")";
        for (i in d) {
            ChartData.labels.push(d[i].time);
            ByteSizeData.data.push(d[i].ByteSize);
            CountData.data.push(d[i].Count);
        }
        ChartData.datasets.push(ByteSizeData);
        ChartData.datasets.push(CountData);
        if($("#"+CanvasId).length > 0) {
            var ctx = document.getElementById(CanvasId).getContext("2d");
            var chart = new Chart(ctx, {type:this.ChartType,data:ChartData});
        }
    }

    incrementData = function(d){
        var data = [];
        CountType = "条";
        ByteSizeType = "b";
        var Count = -1;
        var ByteSize = -1;
        for (s in d) {
            if (d[s].Time > 0){
                if (Count == -1){
                    Count = d[s].Count;
                    ByteSize = d[s].ByteSize;
                    continue;
                }
                var tSize = d[s].ByteSize-ByteSize;
                if( tSize < 0){
                    tSize = 0;
                }
                var tCount = d[s].Count-Count;
                if (tCount < 0){
                    tCount = 0;
                }
                data.push({
                    time: this.TimeFormat(d[s].Time),
                    Count: tCount,
                    ByteSize: tSize,
                });
                Count = d[s].Count;
                ByteSize = d[s].ByteSize;
            }
        }
        return data;
    }

    fullData = function(d){
        var data = [];
        CountType = "条"
        ByteSizeType = "b"
        if (d[0].Count > 100000){
            CountType = "k"
        }
        if (d[0].ByteSize >= 1024000 ){
            ByteSizeType = "kb"
        }

        if (d[0].ByteSize >= 1024000000 ){
            ByteSizeType = "MB"
        }

        if (d[0].ByteSize >= 1024000000000 ){
            ByteSizeType = "GB"
        }

        for (s in d) {
            if (d[s].Time != ""){
                var Count = 0
                if (CountType == "k"){
                    Count =(d[s].Count/1000).toFixed(2)
                }else{
                    Count = d[s].Count
                }
                var ByteSize = 0
                switch (ByteSizeType){
                    case "b":
                        ByteSize = d[s].ByteSize;
                        break
                    case "kb":
                        ByteSize =(d[s].ByteSize/1024).toFixed(2)
                        break
                    case "MB":
                        ByteSize =(d[s].ByteSize/1024000).toFixed(2)
                        break
                    case "GB":
                        ByteSize =(d[s].ByteSize/1024000000).toFixed(2)
                        break
                }

                data.push({
                    time: this.TimeFormat(d[s].Time),
                    Count: Count,
                    ByteSize: ByteSize,
                });
            }
        }
        return data;
    }

    getFlowData() {
        $.post(
            "/flow/get",
            {
                dbname: this.dbName,
                schema: this.schema,
                table_name: this.tableName,
                channelid: this.ChanneId,
                type: this.type,
            },
            function (d, status) {
                if (status != "success") {
                    return false;
                }

                if (this.DisplayFormat == "full"){
                    this.rewrite_data(this.fullData(d));
                }else{
                    this.rewrite_data(this.incrementData(d));
                }

            }, 'json');
    }
}