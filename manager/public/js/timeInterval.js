// JavaScript Document

try {
	if (typeof(timeInterval) == "undefined") {
		var timeInterval = 5000;
	}
} catch(e) {
	var timeInterval = 5000;
}

var timeIntervalBoxHtml = '<div style="position:fixed; right:15px; bottom:5px; text-align:right">';
timeIntervalBoxHtml += '<select id="timeIntervalBox">';
timeIntervalBoxHtml += '<option value="5000"'+(timeInterval==5000?'selected':'')+'>每5秒更新一次</option>';
timeIntervalBoxHtml += '<option value="10000"'+(timeInterval==10000?'selected':'')+'>每10秒更新一次</option>';
timeIntervalBoxHtml += '<option value="30000"'+(timeInterval==30000?'selected':'')+'>每30秒更新一次</option>';
timeIntervalBoxHtml += '<option value="300000"'+(timeInterval==300000?'selected':'')+'>每5分钟更新一次</option>';
timeIntervalBoxHtml += '<option value="0">永不更新</option>';
timeIntervalBoxHtml += '</select><br/>';
timeIntervalBoxHtml += '最后更新时间:<span id="lastUpdateTimeBox">2017-01-11 12:00:00</span>';
timeIntervalBoxHtml += '</div>';
$('body').append(timeIntervalBoxHtml);
function getNowFormatDate() {
	var date = new Date();
	var seperator1 = "-";
	var seperator2 = ":";
	var month = date.getMonth() + 1;
	var strDate = date.getDate();
	if (month >= 1 && month <= 9) {
		month = "0" + month;
	}
	if (strDate >= 0 && strDate <= 9) {
		strDate = "0" + strDate;
	}
	var currentdate = date.getFullYear() + seperator1 + month + seperator1 + strDate
			+ " " + date.getHours() + seperator2 + date.getMinutes()
			+ seperator2 + date.getSeconds();
	return currentdate;
}
function doIntervalFun(){
	IntervalFun();
	$("#lastUpdateTimeBox").text(getNowFormatDate());
}
doIntervalFun();
var timeInterObj;
timeInterObj = window.setInterval(doIntervalFun,timeInterval);
$("#timeIntervalBox").on("change","",function(){
	if($("#timeIntervalBox").val() == 0){
		window.clearInterval(timeInterObj);
		timeInterval = $("#timeIntervalBox").val();
	}else{
		window.clearInterval(timeInterObj);
		timeInterval = $("#timeIntervalBox").val();
		timeInterObj = window.setInterval(doIntervalFun,timeInterval);
	}
});