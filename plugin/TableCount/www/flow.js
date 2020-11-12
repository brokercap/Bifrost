var FlowClass  = {
    dbName: "",
    schema: "",
    tableName: "",
    AgetLength: "tenminute",
    CanvasId: "",
    ChartType: "line",
    CallBack:null,
    Data:[],

    maxCount:0,
    maxByteSize:0,

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
            //color:["#1ab394","#5CACEE"],
            tooltip: {
                trigger: "axis"
            },
            legend: {
                data: ["InsertCount","UpdateCount","DeleteCount","DDLCount","InsertRows","UpdateRows","DeleteRows","CommitCount"]
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
                    name: "InsertCount",
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
                    name: "UpdateCount",
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
                    name: "DeleteCount",
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
                    name: "DDLCount",
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
                    name: "InsertRows",
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
                    name: "UpdateRows",
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
                    name: "DeleteRows",
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
                    name: "CommitCount",
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
                }

            ]
        };
    },

    rewrite_data: function (d) {
        if ($("#" + this.CanvasId).length <= 0) {
            return
        }
        if(d == undefined || d == null){
            return false;
        }
        if (d.length == 0) {
            this.Data = [];
            return false
        }
        this.Data = d;

        var e = echarts.init(document.getElementById(this.CanvasId));
        var a = this.init_data();

        for( var i in d){
            if (d[i].Time == 0 || d[i].Time==""){
                continue;
            }
            a.xAxis[0].data.push(this.TimeFormat(d[i].Time));
            a.series[0].data.push(d[i].InsertCount);
            a.series[1].data.push(d[i].UpdateCount);
            a.series[2].data.push(d[i].DeleteCount);
            a.series[3].data.push(d[i].DDLCount);
            a.series[4].data.push(d[i].InsertRows);
            a.series[5].data.push(d[i].UpdateRows);
            a.series[6].data.push(d[i].DeleteRows);
            a.series[7].data.push(d[i].CommitCount);
        }
        e.setOption(a);
        $(window).resize(e.resize);
        d = null;

    },

    getFlowData: function () {
        if (this.dbName==""){
            return;
        }
        var obj = this;
        $.get(
            "/bifrost/plugin/TableCount/flow/get",
            {
                DbName: this.dbName,
                SchemaName: this.schema,
                TableName: this.tableName,
                Type: this.AgetLength,
            },
            function (d, status) {
                if (status != "success") {
                    return false;
                }
                obj.rewrite_data(obj.rewrite_data(d.data));
                if(obj.CallBack != null){
                    obj.CallBack();
                }
            }, 'json');
    },
}