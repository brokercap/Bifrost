$("#KeyConfig,#ValueConfig").focus(
    function(){
        OnFoucsInputId = $(this).attr("id");
    }
);

function doGetParam(){
    var data = {};
    var AddEventType = $("input[name='AddEventType']:checked").val();

    var addToServerDataType = $("#addToServerDataType").val();
    var KeyConfig = $("#KeyConfig").val();
    var ValueConfig = $("#ValueConfig").val();
    if (KeyConfig==""){
        alert("Key must be not empty!");
        return false;
    }
    if (addToServerDataType == "string" && ValueConfig==""){
        alert("DataType==string,ValueConfig muest be!");
        return false;
    }

    if (addToServerDataType == "string"){
        var valRule = ToServerList[addToServerKey].TypeAndRule.TypeList[addToServerType].Val;
        if (valRule == "json"){
            try {
                var obj= JSON.parse(ValueConfig);
                if(typeof obj != 'object' || !obj ){
                    alert("ValueConfig rule:"+valRule);
                    return false;
                }
            } catch(e) {
                alert("ValueConfig rule:"+valRule);
                return false;
            }
        }
        if (valRule != ""){
            var valp=new RegExp(valRule);
            if (valp.test(ValueConfig) == false){
                alert("ValueConfig rule:"+valRule);
                return false;
            }
        }
    }
    var Expir = $("#Expir").val();
    if (Expir != "" && Expir != null && isNaN(Expir)){
        alert("Expir must be int");
        return false;
    }
    var AddSchemaName = $("input[name='AddSchemaName']:checked").val();
    var AddTableName = $("input[name='AddTableName']:checked").val();
    data["addToServerDataType"] = addToServerDataType;
    data["KeyConfig"] = KeyConfig;
    data["ValueConfig"] = ValueConfig;
    return data;
}