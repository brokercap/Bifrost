var FlowClass  = {
    CountType: "条",
    ByteSizeType: "b",
    dbName: "",
    schema: "",
    tableName: "",
    ChanneId: "",
    AgetLength: "tenminute",
    CanvasId: "",
    ChartType: "line",
    DisplayFormat: "increment",//increment,full;

    CountSum:0,
    ByteSizeSum:0,
    CallBack:null,
    Data:[],

    maxCount:0,
    maxByteSize:0,

    getCountSum:function () {
        return this.CountSum;
    },

    getByteSizeSum:function () {
        return this.ByteSizeSum;
    },

    setDbName: function (dbName) {
        this.dbName = dbName;
    },
    setSchema: function (schema) {
        this.schema = schema;
    },

    setChanneId:function (ChanneId) {
        this.ChanneId = ChanneId;
    },
    setTableName: function (tableName) {
        this.tableName = tableName;
    },

    setAgetLength: function (AgetLength) {
        this.AgetLength = AgetLength;
    },
    setCanvasId: function (CanvasId) {
        this.CanvasId = CanvasId;
    },

    setDisplayFormat: function (DisplayFormat) {
        this.DisplayFormat = DisplayFormat;
    },

    setCallBackFun: function (f) {
        if (typeof(f) == "function"){
            this.CallBack = f;
        }
    },

    getData:function () {
        return this.Data;
    },

    add0: function (m) {
        return m < 10 ? '0' + m : m
    },

    TimeFormat: function (timeUnix) {
        var time = new Date(parseInt(timeUnix) * 1000);
        var y = time.getFullYear();
        var m = time.getMonth();
        var d = time.getDate();
        var h = time.getHours();
        var mm = time.getMinutes();
        var s = time.getSeconds();
        return this.add0(h) + ':' + this.add0(mm) + ':' + this.add0(s);
        //return y + '-' + this.add0(m) + '-' + this.add0(d) + ' ' + this.add0(h) + ':' + this.add0(mm) + ':' + this.add0(s);
    },

    init_data: function (ByteLable,CountLable) {
        return {
            color:["#1ab394","#5CACEE"],
            tooltip: {
                trigger: "axis"
            },
            legend: {
                data: [CountLable,ByteLable]
            },
            calculable: !0,
            xAxis: [{
                type: "category",
                boundaryGap: !1,
                data: []
            }],
            yAxis: [{
                type: "value"
            }],
            series: [
                {
                    name: CountLable,
                    type: "line",
                    data: [],
                    markPoint: {
                        data: [{
                            type: "max",
                            name: "最大值"
                        },
                            {
                                type: "min",
                                name: "最小值"
                            }]
                    }

                },
                {
                name: ByteLable,
                type: "line",
                data: [],
                markPoint: {
                    data: [{
                        type: "max",
                        name: "最大值"
                    },
                        {
                            type: "min",
                            name: "最小值"
                        }]
                }
            }]
        };
    },

    rewrite_data: function (d) {
        if ($("#" + this.CanvasId).length <= 0) {
            return
        }
        if (d.length == 0) {
            this.Data = [];
            return false
        }
        this.Data = d;

        var CountDivideNumber = 1;
        var ByteSizeDivideNumber = 1;

        if (this.maxCount > 500000) {
            this.CountType = "k";
            CountDivideNumber = 1000;
        }

        if (this.maxByteSize >= 1024000) {
            ByteSizeType = "kb";
            ByteSizeDivideNumber = 1024;
        }

        if (d[0].ByteSize >= 1024000000) {
            ByteSizeType = "MB"
            ByteSizeDivideNumber = 1024 * 1024;
        }

        if (d[0].ByteSize >= 1024000000000) {
            ByteSizeType = "GB"
            ByteSizeDivideNumber = 1024 * 1024 * 1024;
        }

        var ByteLable = "ByteSize(" + ByteSizeType + ")";
        var CountLable = "Count(" + CountType + ")";
        var e = echarts.init(document.getElementById(this.CanvasId));
        var a = this.init_data(ByteLable,CountLable);

        for( var i in d){
            a.xAxis[0].data.push(d[i].time);
            a.series[0].data.push(d[i].Count/CountDivideNumber);
            a.series[1].data.push((d[i].ByteSize / ByteSizeDivideNumber).toFixed(2));
        }
        e.setOption(a);
        $(window).resize(e.resize);
        d = null;

    },

    incrementData: function (d) {
        var data = [];
        CountType = "条";
        ByteSizeType = "b";
        var Count = -1;
        var ByteSize = -1;
        var lasttime = -1;
        for (s in d) {
            if (d[s].Time > 0) {
                if (Count == -1) {
                    Count = d[s].Count;
                    ByteSize = d[s].ByteSize;
                    continue;
                }

                if (lasttime == 0){
                    data.push({
                        time: this.TimeFormat(d[s].Time-5),
                        Count: 0,
                        ByteSize: 0,
                    });
                }
                lasttime = d[s].Time;
                var tSize = d[s].ByteSize - ByteSize;
                if (tSize < 0) {
                    tSize = 0;
                }
                var tCount = d[s].Count - Count;
                if (tCount < 0) {
                    tCount = 0;
                }
                data.push({
                    time: this.TimeFormat(d[s].Time),
                    Count: tCount,
                    ByteSize: tSize,
                });
                Count = d[s].Count;
                ByteSize = d[s].ByteSize;
                this.ByteSizeSum += tSize;
                this.CountSum += tCount;
                if (Count > this.maxCount){
                    this.maxCount = d[s].Count;
                }
                if (ByteSize > this.maxByteSize){
                    this.maxByteSize = d[s].ByteSize;
                }

            }else{
                Count = 0;
                ByteSize = 0;
                lasttime = 0;
            }
        }
        return data;
    },

    fullData: function (d) {
        var data = [];
        for (s in d) {
            if (d[s].Time != "") {
                data.push({
                    time: this.TimeFormat(d[s].Time),
                    Count: d[s].Count,
                    ByteSize: d[s].ByteSize,
                });

                this.ByteSizeSum = d[s].ByteSize;
                this.CountSum = d[s].Count;

                this.maxCount = d[s].Count;
                this.maxByteSize = d[s].ByteSize;
            }
        }
        return data;
    },

    getFlowData: function () {
        var obj = this;
        this.ByteSizeSum = 0;
        this.CountSum = 0;
        $.post(
            "/flow/get",
            {
                dbname: this.dbName,
                schema: this.schema,
                table_name: this.tableName,
                channelid: this.ChanneId,
                type: this.AgetLength,
            },
            function (d, status) {
                if (status != "success") {
                    return false;
                }

                if (obj.DisplayFormat == "full") {
                    obj.rewrite_data(obj.fullData(d));
                } else {
                    obj.rewrite_data(obj.incrementData(d));
                }
                if(obj.CallBack != null){
                    obj.CallBack();
                }
            }, 'json');
    },
}