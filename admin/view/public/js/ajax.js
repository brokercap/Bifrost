function Ajax(method,url,data,callbackFun,sync) {
    var async;
    if ( sync == undefined || sync == null || sync != false){
        async = true;
    }else{
        async = false;
    }
    if (method.toUpperCase() == "GET"){
        AjaxGet(method,url,data,callbackFun,async);
        return true
    }
    $.ajax({
        url:url,  //请求的URL
        timeout : 5000, //超时时间设置，单位毫秒
        type : method,  //请求方式，get或post
        data :JSON.stringify(data),  //请求所传参数，json格式
        dataType:'json',//返回的数据格式
        async:async,
        processData: false,
        contentType: "application/json",
        success:function(callbackData){ //请求成功的回调函数
            if (callbackData.hasOwnProperty("status")){
                if (callbackData.status == -1) {
                    alert(callbackData.msg);
                    return;
                }
            }
            callbackFun(callbackData);
        },
        complete : function(XMLHttpRequest,status){ //请求完成后最终执行参数
            if( status != 'success' ){
                alert("reqeust error, reqeust status : "+status);
                return false;
            }
        }
    });
}

function AjaxGet(method,url,data,callbackFun,async) {
    $.ajax({
        url:url,  //请求的URL
        timeout : 5000, //超时时间设置，单位毫秒
        type : method,  //请求方式，get或post
        data :data,  //请求所传参数，json格式
        dataType:'json',//返回的数据格式
        async:async,
        success:function(callbackData){ //请求成功的回调函数
            if (callbackData.hasOwnProperty("status")){
                if (callbackData.status == -1) {
                    alert(callbackData.msg);
                    return;
                }
            }
            callbackFun(callbackData);
        },
        complete : function(XMLHttpRequest,status){ //请求完成后最终执行参数
            if( status != 'success' ){
                alert("reqeust error, reqeust status : "+status);
                return false;
            }
        }
    });
}