//首页导引
function IndexGruid() {
    $("#header_nav").find("li").eq(1).attr({"data-intro":"2. 点击添加 数据源","data-position":"bottom"});
    $("#header_nav").find("li").eq(2).attr({"data-intro":"1. 点击添加 目标库,数据需要同步到哪些地方","data-position":"bottom"});
}

//数据源界面引导
function DbSourceGruid() {
    $("#checkUriBtn").attr({"data-intro":"1. 验证连接配置是否正确及帐号是否有足够权限","data-position":"right"});
    $("#addNewDBBtn").attr({"data-intro":"2. 点击添加 数据源,添加完之后，可以对表进行同步配置","data-position":"right"});
    $("#dbListContair").find("tr").eq(0).find("td").eq(11).find("a").eq(1).find("button").attr({"data-intro":"3. 点击进入到 表同步配置界面","data-position":"bottom"});
    $("#dbListContair").find("tr").eq(0).children(2).find(".startDB").attr({"data-intro":"4. 表同步设置完之后，回到数据源界面，点击 Start 开始增量同步","data-position":"left"});
}

//目标库界面引导
function ToserverListGruid() {
    $(".col-lg-12").find(".table-striped thead tr").eq(0).find("th").eq(5).attr({"data-intro":"线程池最大连接数","data-position":"top"});
    $(".col-lg-12").find(".table-striped thead tr").eq(0).find("th").eq(6).attr({"data-intro":"当前正在执行同步的连接数","data-position":"bottom"});
    $(".col-lg-12").find(".table-striped thead tr").eq(0).find("th").eq(7).attr({"data-intro":"空闲连接数","data-position":"top"});
    $("#toserverkey").attr({"data-intro":"1. ToServerKey 在表配置同步的时候，需要用到，不要中文！","data-position":"right"});
    $("#conn_type").attr({"data-intro":"2. 选择是哪一个目标库，版本是插件版本，并不是要求 目标库 哪一个版本","data-position":"right"});
    $("#checkUriBtn").attr({"data-intro":"3. 验证连接配置是否正确","data-position":"bottom"});
    $("#addNewToServerBtn").attr({"data-intro":"4. 提交添加 目标库","data-position":"right"});
}

//数据源详情 表同步配置界面 引导
function DbDetailGruid() {
    $("#DatabaseListContair").attr({"data-intro":"1. 选择数据库，背景为绿色则为选中状态","data-position":"top"});
    $("#TableListContair").attr({"data-intro":"2. 点击 ADD 按钮 让表和通道绑定！ 再点击表名 变为绿色！！才能设置表同步！！","data-position":"top"});
    $("#TableListContair input[name='table_check_name']").eq(0).attr({"data-intro":"备： 复选框 是用于 批量绑定通道和删除，要设置表同步，请击 ADD 按钮后再点击 表名，背景成为绿色","data-position":"right"});
    $("#addToServerContair").attr({"data-intro":"3. 配置 需要同步到哪一个目标库里，每个目标库，不同的参数配置！","data-position":"top"});
}


function GetUrlRelativePath()
{
    var url = document.location.toString();
    var arrUrl = url.split("//");
    var start = arrUrl[1].indexOf("/");
    var relUrl = arrUrl[1].substring(start);//stop省略，截取从start开始到结尾的所有字符
    if(relUrl.indexOf("?") != -1){
        relUrl = relUrl.split("?")[0];
    }
    return relUrl;
}

var gruidBool = false;

if(document.URL.indexOf("/db/index") != -1){
    DbSourceGruid();
    gruidBool = true;
}else if(document.URL.indexOf("/toserver/index") != -1){
    ToserverListGruid();
    gruidBool = true;
}else if(document.URL.indexOf("/db/detail") != -1){
    DbDetailGruid();
    gruidBool = true;
}else{
    var RelativePath = GetUrlRelativePath();
    if (RelativePath == "/" || RelativePath == "/#"){
        IndexGruid();
        gruidBool = true;
    }
}

if(gruidBool){
    $("#header_nav").prepend("<li><a href='#' onclick='GruidStart();' class=\"dropdown-toggle\"><i class=\"fa\"></i>操作引导</a></li>")
}

function GruidStart() {

    if(gruidBool) {
        $('body').chardinJs({
            'attribute': 'data-intro'
        });
        $('body').chardinJs("start");
    }
}